package management

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// CreateCampaign atomically reserves budget, persists the campaign, and queues hot-path propagation.
func (s *Service) CreateCampaign(ctx context.Context, spec CampaignCreateSpec) (uuid.UUID, error) {
	if err := validateDaypartHours(spec.DaypartHours); err != nil {
		return uuid.Nil, err
	}
	if err := validateSchedule(spec.StartAt, spec.EndAt); err != nil {
		return uuid.Nil, err
	}

	campaignID, _ := uuid.NewV7()
	now := time.Now()
	initialStatus := resolveScheduleStatus(now, spec.StartAt, spec.EndAt)

	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		existing, err := q.GetLedgerByHashForUpdate(ctx, pgtype.Text{String: spec.IdempotencyKey, Valid: true})
		if err == nil {
			if existing.CampaignID.Valid {
				campaignID = uuid.UUID(existing.CampaignID.Bytes)
				return nil
			}
			return fmt.Errorf("incomplete idempotency ledger row for key %q", spec.IdempotencyKey)
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("idempotency lookup failed: %w", err)
		}
		cust, err := q.GetCustomerForUpdate(ctx, ads.ToUUID(spec.CustomerID))
		if err != nil {
			return fmt.Errorf("customer not found: %w", err)
		}
		if cust.Balance+cust.AllowedOverdraft < spec.BudgetLimit {
			return fmt.Errorf("insufficient balance")
		}

		var brandIDParam pgtype.UUID
		brandFcapKey := "fcap:c:" + campaignID.String()
		if spec.BrandID != nil {
			brand, err := q.GetBrand(ctx, ads.ToUUID(*spec.BrandID))
			if err != nil {
				return fmt.Errorf("brand not found: %w", err)
			}
			if uuid.UUID(brand.CustomerID.Bytes) != spec.CustomerID {
				return fmt.Errorf("brand belongs to another customer")
			}
			brandIDParam = ads.ToUUID(*spec.BrandID)
			brandFcapKey = "fcap:b:" + spec.BrandID.String()
		}

		var templateIDParam pgtype.UUID
		if spec.TemplateID != nil {
			templateIDParam = ads.ToUUID(*spec.TemplateID)
		}

		if _, err = q.UpdateCustomerBalanceManagement(ctx, db.UpdateCustomerBalanceManagementParams{
			ID:      ads.ToUUID(spec.CustomerID),
			Balance: -spec.BudgetLimit,
		}); err != nil {
			return err
		}

		_, err = q.CreateCampaign(ctx, db.CreateCampaignParams{
			ID:              ads.ToUUID(campaignID),
			Name:            spec.Name,
			BudgetLimit:     spec.BudgetLimit,
			Status:          initialStatus,
			CustomerID:      ads.ToUUID(spec.CustomerID),
			PacingMode:      spec.PacingMode,
			DailyBudget:     spec.DailyBudget,
			Timezone:        spec.Timezone,
			FreqLimit:       pgtype.Int4{Int32: spec.FreqLimit, Valid: true},
			FreqWindow:      pgtype.Int4{Int32: spec.FreqWindow, Valid: true},
			TargetCountries: countriesOrEmpty(spec.TargetCountries),
			BrandID:         brandIDParam,
			BrandFcapKey:    brandFcapKey,
			StartAt:         toTimestamptz(spec.StartAt),
			EndAt:           toTimestamptz(spec.EndAt),
			DaypartHours:    daypartOrEmpty(spec.DaypartHours),
			TemplateID:      templateIDParam,
		})
		if err != nil {
			return err
		}

		_, err = q.CreateLedgerEntry(ctx, db.CreateLedgerEntryParams{
			CustomerID:      ads.ToUUID(spec.CustomerID),
			CampaignID:      ads.ToUUID(campaignID),
			Amount:          spec.BudgetLimit,
			Type:            db.LedgerTypeFREEZE,
			IdempotencyHash: pgtype.Text{String: spec.IdempotencyKey, Valid: true},
		})
		if err != nil {
			return err
		}

		err = q.CreateStatusHistory(ctx, db.CreateStatusHistoryParams{
			CampaignID: ads.ToUUID(campaignID),
			NewStatus:  initialStatus,
			Reason:     pgtype.Text{String: "Campaign creation", Valid: true},
		})
		if err != nil {
			return err
		}

		s.AuditLog(ctx, q, uuid.Nil, "CREATE_CAMPAIGN", "campaign", &campaignID, map[string]any{
			"name":          spec.Name,
			"budget_limit":  spec.BudgetLimit,
			"status":        initialStatus,
			"start_at":      spec.StartAt,
			"end_at":        spec.EndAt,
			"daypart_hours": spec.DaypartHours,
		}, map[string]any{"idempotency_key": spec.IdempotencyKey})

		return s.emitCampaignLifecycleOutbox(ctx, q, campaignID, initialStatus, spec.BudgetLimit)
	})
	return campaignID, err
}

// emitCampaignLifecycleOutbox enqueues the Redis side effect matching a campaign's initial or transitioned status.
func (s *Service) emitCampaignLifecycleOutbox(ctx context.Context, q db.Querier, campaignID uuid.UUID, status db.CampaignStatusType, budgetLimit int64) error {
	switch status {
	case db.CampaignStatusTypeACTIVE:
		payload, _ := json.Marshal(CampaignPayload{CampaignID: campaignID.String(), BudgetLimit: budgetLimit})
		_, err := q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{EventType: "CREATE_CAMPAIGN", Payload: payload})
		return err
	case db.CampaignStatusTypePAUSED:
		payload, _ := json.Marshal(CampaignPayload{CampaignID: campaignID.String()})
		_, err := q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{EventType: "PAUSE_CAMPAIGN", Payload: payload})
		return err
	default:
		return nil
	}
}

// PauseCampaign stops delivery for an active campaign and notifies the hot path via outbox.
func (s *Service) PauseCampaign(ctx context.Context, campaignID uuid.UUID, reason string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		camp, err := q.GetCampaignForUpdate(ctx, ads.ToUUID(campaignID))
		if err != nil {
			return fmt.Errorf("campaign not found: %w", err)
		}
		if camp.Status == db.CampaignStatusTypePAUSED {
			return nil
		}
		if camp.Status != db.CampaignStatusTypeACTIVE {
			return fmt.Errorf("campaign cannot be paused in status %s", camp.Status)
		}

		_, err = q.PauseCampaign(ctx, ads.ToUUID(campaignID))
		if err != nil {
			return err
		}
		err = q.CreateStatusHistory(ctx, db.CreateStatusHistoryParams{
			CampaignID: ads.ToUUID(campaignID),
			OldStatus:  db.NullCampaignStatusType{CampaignStatusType: camp.Status, Valid: true},
			NewStatus:  db.CampaignStatusTypePAUSED,
			Reason:     pgtype.Text{String: reason, Valid: reason != ""},
		})
		if err != nil {
			return err
		}

		var uid uuid.UUID
		if u, ok := GetUser(ctx); ok {
			uid = u.UserID
		}
		s.AuditLog(ctx, q, uid, "PAUSE_CAMPAIGN", "campaign", &campaignID, map[string]any{"reason": reason}, nil)

		payload, _ := json.Marshal(CampaignPayload{CampaignID: campaignID.String()})
		_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{EventType: "PAUSE_CAMPAIGN", Payload: payload})
		return err
	})
}

// ResumeCampaign reactivates a paused campaign when schedule and balance constraints allow.
func (s *Service) ResumeCampaign(ctx context.Context, campaignID uuid.UUID, reason string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		camp, err := q.GetCampaignForUpdate(ctx, ads.ToUUID(campaignID))
		if err != nil {
			return fmt.Errorf("campaign not found: %w", err)
		}
		if camp.Status != db.CampaignStatusTypePAUSED {
			return fmt.Errorf("campaign is not paused")
		}

		now := time.Now()
		var startAt, endAt *time.Time
		if camp.StartAt.Valid {
			startAt = &camp.StartAt.Time
		}
		if camp.EndAt.Valid {
			endAt = &camp.EndAt.Time
		}
		if resolveScheduleStatus(now, startAt, endAt) != db.CampaignStatusTypeACTIVE {
			return fmt.Errorf("campaign is outside scheduled delivery window")
		}

		_, err = q.ResumeCampaign(ctx, ads.ToUUID(campaignID))
		if err != nil {
			return err
		}
		err = q.CreateStatusHistory(ctx, db.CreateStatusHistoryParams{
			CampaignID: ads.ToUUID(campaignID),
			OldStatus:  db.NullCampaignStatusType{CampaignStatusType: camp.Status, Valid: true},
			NewStatus:  db.CampaignStatusTypeACTIVE,
			Reason:     pgtype.Text{String: reason, Valid: reason != ""},
		})
		if err != nil {
			return err
		}

		var uid uuid.UUID
		if u, ok := GetUser(ctx); ok {
			uid = u.UserID
		}
		s.AuditLog(ctx, q, uid, "RESUME_CAMPAIGN", "campaign", &campaignID, map[string]any{"reason": reason}, nil)

		payload, _ := json.Marshal(CampaignPayload{CampaignID: campaignID.String(), BudgetLimit: camp.BudgetLimit})
		_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{EventType: "RESUME_CAMPAIGN", Payload: payload})
		return err
	})
}

// UpdateCampaignSchedule changes delivery windows and may auto-pause or resume based on the new schedule.
func (s *Service) UpdateCampaignSchedule(ctx context.Context, campaignID uuid.UUID, startAt, endAt *time.Time, daypartHours []int16) error {
	if err := validateDaypartHours(daypartHours); err != nil {
		return err
	}
	if err := validateSchedule(startAt, endAt); err != nil {
		return err
	}

	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		locked, err := q.GetCampaignForUpdate(ctx, ads.ToUUID(campaignID))
		if err != nil {
			return err
		}
		_, err = q.UpdateCampaignSchedule(ctx, db.UpdateCampaignScheduleParams{
			ID:           ads.ToUUID(campaignID),
			StartAt:      toTimestamptz(startAt),
			EndAt:        toTimestamptz(endAt),
			DaypartHours: daypartOrEmpty(daypartHours),
		})
		if err != nil {
			return err
		}

		var uid uuid.UUID
		if u, ok := GetUser(ctx); ok {
			uid = u.UserID
		}
		s.AuditLog(ctx, q, uid, "UPDATE_CAMPAIGN_SCHEDULE", "campaign", &campaignID, map[string]any{
			"start_at": startAt, "end_at": endAt, "daypart_hours": daypartHours,
		}, nil)

		payload, _ := json.Marshal(map[string]any{
			"campaign_id":   campaignID.String(),
			"start_at":      startAt,
			"end_at":        endAt,
			"daypart_hours": daypartHours,
		})
		_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{EventType: "UPDATE_CAMPAIGN_SCHEDULE", Payload: payload})
		if err != nil {
			return err
		}

		desired := resolveScheduleStatus(time.Now(), startAt, endAt)
		if desired == db.CampaignStatusTypePAUSED && locked.Status == db.CampaignStatusTypeACTIVE {
			return s.transitionCampaignStatus(ctx, q, campaignID, locked.Status, db.CampaignStatusTypePAUSED, "schedule_window", locked.BudgetLimit)
		}
		if desired == db.CampaignStatusTypeACTIVE && locked.Status == db.CampaignStatusTypePAUSED {
			return s.transitionCampaignStatus(ctx, q, campaignID, locked.Status, db.CampaignStatusTypeACTIVE, "schedule_window", locked.BudgetLimit)
		}
		return nil
	})
}

// transitionCampaignStatus updates status, records history, and emits the matching lifecycle outbox event.
func (s *Service) transitionCampaignStatus(ctx context.Context, q db.Querier, campaignID uuid.UUID, old, new db.CampaignStatusType, reason string, budget int64) error {
	_, err := q.UpdateCampaignStatus(ctx, db.UpdateCampaignStatusParams{
		ID:     ads.ToUUID(campaignID),
		Status: new,
	})
	if err != nil {
		return err
	}
	err = q.CreateStatusHistory(ctx, db.CreateStatusHistoryParams{
		CampaignID: ads.ToUUID(campaignID),
		OldStatus:  db.NullCampaignStatusType{CampaignStatusType: old, Valid: true},
		NewStatus:  new,
		Reason:     pgtype.Text{String: reason, Valid: true},
	})
	if err != nil {
		return err
	}
	return s.emitCampaignLifecycleOutbox(ctx, q, campaignID, new, budget)
}

// CreateCampaignTemplate stores a reusable campaign preset for a customer.
func (s *Service) CreateCampaignTemplate(ctx context.Context, customerID uuid.UUID, name string, budgetLimit int64, pacing db.PacingModeType, dailyBudget int64, timezone string, freqLimit, freqWindow int32, targetCountries []string, brandID *uuid.UUID, daypartHours []int16) (uuid.UUID, error) {
	if err := validateDaypartHours(daypartHours); err != nil {
		return uuid.Nil, err
	}
	templateID, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, err
	}

	var brandParam pgtype.UUID
	if brandID != nil {
		brandParam = ads.ToUUID(*brandID)
	}

	_, err = db.New(s.pool).CreateCampaignTemplate(ctx, db.CreateCampaignTemplateParams{
		ID:              ads.ToUUID(templateID),
		CustomerID:      ads.ToUUID(customerID),
		Name:            name,
		BudgetLimit:     budgetLimit,
		PacingMode:      pacing,
		DailyBudget:     dailyBudget,
		Timezone:        timezone,
		FreqLimit:       freqLimit,
		FreqWindow:      freqWindow,
		TargetCountries: countriesOrEmpty(targetCountries),
		BrandID:         brandParam,
		DaypartHours:    daypartOrEmpty(daypartHours),
	})
	return templateID, err
}

// ListCampaignTemplates returns paginated templates for a customer's campaign library.
func (s *Service) ListCampaignTemplates(ctx context.Context, customerID uuid.UUID, limit, offset int32) ([]CampaignTemplateDTO, int64, error) {
	q := db.New(s.pool)
	total, err := q.CountCampaignTemplates(ctx, ads.ToUUID(customerID))
	if err != nil || total == 0 {
		return []CampaignTemplateDTO{}, total, err
	}
	rows, err := q.ListCampaignTemplates(ctx, db.ListCampaignTemplatesParams{
		CustomerID: ads.ToUUID(customerID),
		Limit:      limit,
		Offset:     offset,
	})
	if err != nil {
		return nil, 0, err
	}
	res := make([]CampaignTemplateDTO, len(rows))
	for i, r := range rows {
		res[i] = templateToDTO(r)
	}
	return res, total, nil
}

// CreateCampaignFromTemplate instantiates a live campaign from a stored template with optional overrides.
func (s *Service) CreateCampaignFromTemplate(ctx context.Context, templateID uuid.UUID, customerID uuid.UUID, name string, budgetLimit *int64, idempotencyKey string) (uuid.UUID, error) {
	tmpl, err := db.New(s.pool).GetCampaignTemplate(ctx, ads.ToUUID(templateID))
	if err != nil {
		return uuid.Nil, fmt.Errorf("template not found: %w", err)
	}
	if uuid.UUID(tmpl.CustomerID.Bytes) != customerID {
		return uuid.Nil, fmt.Errorf("template belongs to another customer")
	}

	limit := tmpl.BudgetLimit
	if budgetLimit != nil {
		limit = *budgetLimit
	}
	if name == "" {
		name = tmpl.Name
	}

	var brandID *uuid.UUID
	if tmpl.BrandID.Valid {
		id := uuid.UUID(tmpl.BrandID.Bytes)
		brandID = &id
	}

	return s.CreateCampaign(ctx, CampaignCreateSpec{
		CustomerID:      customerID,
		BrandID:         brandID,
		Name:            name,
		BudgetLimit:     limit,
		PacingMode:      tmpl.PacingMode,
		DailyBudget:     tmpl.DailyBudget,
		Timezone:        tmpl.Timezone,
		FreqLimit:       tmpl.FreqLimit,
		FreqWindow:      tmpl.FreqWindow,
		TargetCountries: tmpl.TargetCountries,
		DaypartHours:    tmpl.DaypartHours,
		TemplateID:      &templateID,
		IdempotencyKey:  idempotencyKey,
	})
}

// SaveCampaignAsTemplate snapshots an existing campaign configuration as a reusable template.
func (s *Service) SaveCampaignAsTemplate(ctx context.Context, campaignID uuid.UUID, templateName string) (uuid.UUID, error) {
	camp, err := s.GetCampaign(ctx, campaignID)
	if err != nil {
		return uuid.Nil, err
	}
	if templateName == "" {
		templateName = camp.Name + " template"
	}
	var brandID *uuid.UUID
	if camp.BrandID.Valid {
		id := uuid.UUID(camp.BrandID.Bytes)
		brandID = &id
	}
	hours := camp.DaypartHours
	if hours == nil {
		hours = []int16{}
	}
	return s.CreateCampaignTemplate(ctx,
		uuid.UUID(camp.CustomerID.Bytes),
		templateName,
		camp.BudgetLimit,
		camp.PacingMode,
		camp.DailyBudget,
		camp.Timezone,
		camp.FreqLimit.Int32,
		camp.FreqWindow.Int32,
		camp.TargetCountries,
		brandID,
		hours,
	)
}

// UpsertBrandCreative creates a weighted landing URL variant and queues a Redis sync via outbox.
func (s *Service) UpsertBrandCreative(ctx context.Context, brandID uuid.UUID, name, landingURL string, weight int32, status string) (uuid.UUID, error) {
	if weight <= 0 {
		return uuid.Nil, fmt.Errorf("weight must be positive")
	}
	if status == "" {
		status = "ACTIVE"
	}
	if status != "ACTIVE" && status != "PAUSED" {
		return uuid.Nil, fmt.Errorf("status must be ACTIVE or PAUSED")
	}

	creativeID, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, err
	}

	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		if _, err := q.GetBrand(ctx, ads.ToUUID(brandID)); err != nil {
			return fmt.Errorf("brand not found: %w", err)
		}
		_, err := q.CreateBrandCreative(ctx, db.CreateBrandCreativeParams{
			ID:         ads.ToUUID(creativeID),
			BrandID:    ads.ToUUID(brandID),
			Name:       name,
			LandingUrl: landingURL,
			Weight:     weight,
			Status:     status,
		})
		if err != nil {
			return err
		}
		return s.emitBrandCreativesOutbox(ctx, q, brandID)
	})
	return creativeID, err
}

// ListBrandCreatives returns active and paused creatives for a brand.
func (s *Service) ListBrandCreatives(ctx context.Context, brandID uuid.UUID) ([]BrandCreativeDTO, error) {
	rows, err := db.New(s.pool).ListBrandCreatives(ctx, ads.ToUUID(brandID))
	if err != nil {
		return nil, err
	}
	res := make([]BrandCreativeDTO, len(rows))
	for i, r := range rows {
		res[i] = creativeToDTO(r)
	}
	return res, nil
}

// UpdateBrandCreative edits a creative and triggers hot-path resync via outbox.
func (s *Service) UpdateBrandCreative(ctx context.Context, creativeID uuid.UUID, name, landingURL string, weight int32, status string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		existing, err := q.GetBrandCreative(ctx, ads.ToUUID(creativeID))
		if err != nil {
			return fmt.Errorf("creative not found: %w", err)
		}
		_, err = q.UpdateBrandCreative(ctx, db.UpdateBrandCreativeParams{
			ID:         ads.ToUUID(creativeID),
			Name:       name,
			LandingUrl: landingURL,
			Weight:     weight,
			Status:     status,
		})
		if err != nil {
			return err
		}
		return s.emitBrandCreativesOutbox(ctx, q, uuid.UUID(existing.BrandID.Bytes))
	})
}

// DeleteBrandCreative removes a creative and triggers hot-path resync via outbox.
func (s *Service) DeleteBrandCreative(ctx context.Context, creativeID uuid.UUID) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		existing, err := q.GetBrandCreative(ctx, ads.ToUUID(creativeID))
		if err != nil {
			return fmt.Errorf("creative not found: %w", err)
		}
		if err := q.DeleteBrandCreative(ctx, ads.ToUUID(creativeID)); err != nil {
			return err
		}
		return s.emitBrandCreativesOutbox(ctx, q, uuid.UUID(existing.BrandID.Bytes))
	})
}

// emitBrandCreativesOutbox queues a Redis refresh of weighted creatives for a brand.
func (s *Service) emitBrandCreativesOutbox(ctx context.Context, q db.Querier, brandID uuid.UUID) error {
	payload, _ := json.Marshal(map[string]string{"brand_id": brandID.String()})
	_, err := q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{EventType: "SYNC_BRAND_CREATIVES", Payload: payload})
	return err
}

// ProcessScheduleTick claims and applies schedule-driven status changes for due campaigns.
func (s *Service) ProcessScheduleTick(ctx context.Context) error {
	opCtx, cancel := workerContext(ctx, workerBatchTimeout)
	defer cancel()

	for i := int32(0); i < 200; i++ {
		done, err := s.processNextScheduledCampaign(opCtx)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
	return nil
}

// processNextScheduledCampaign locks one scheduled campaign and returns whether the queue is empty.
func (s *Service) processNextScheduledCampaign(ctx context.Context) (done bool, err error) {
	var campID uuid.UUID
	var desired db.CampaignStatusType

	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		camp, err := q.ClaimScheduledCampaignForUpdate(ctx)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				done = true
				return nil
			}
			return err
		}

		var startAt, endAt *time.Time
		if camp.StartAt.Valid {
			startAt = &camp.StartAt.Time
		}
		if camp.EndAt.Valid {
			endAt = &camp.EndAt.Time
		}
		desired = resolveScheduleStatus(time.Now(), startAt, endAt)
		if desired == camp.Status {
			return nil
		}
		campID = uuid.UUID(camp.ID.Bytes)
		return nil
	})
	if err != nil || done || campID == uuid.Nil {
		return done, err
	}

	var opErr error
	if desired == db.CampaignStatusTypeACTIVE {
		opErr = s.ResumeCampaign(ctx, campID, "schedule_auto_resume")
	} else {
		opErr = s.PauseCampaign(ctx, campID, "schedule_auto_pause")
	}
	if opErr != nil {
		slog.Warn("schedule tick skipped campaign", "campaign_id", campID, "error", opErr)
	}
	return false, nil
}
