package ads

import (
	"context"
	"errors"
	"testing"
	"time"

	"espx/internal/domain"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

type mockBatch struct {
	driver.Batch
	appendFn func(args ...any) error
	sendFn   func() error
}

func (m *mockBatch) Append(v ...any) error {
	if m.appendFn != nil {
		return m.appendFn(v...)
	}
	return nil
}

func (m *mockBatch) Send() error {
	if m.sendFn != nil {
		return m.sendFn()
	}
	return nil
}

type mockConn struct {
	driver.Conn
	prepareBatchFn func(ctx context.Context, query string) (driver.Batch, error)
	closeFn        func() error
}

func (m *mockConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	if m.prepareBatchFn != nil {
		return m.prepareBatchFn(ctx, query)
	}
	return nil, nil
}

func (m *mockConn) Close() error {
	if m.closeFn != nil {
		return m.closeFn()
	}
	return nil
}

func TestClickHouseStore_StoreBatch_PartialFailureDeduplication(t *testing.T) {
	evt1 := &domain.Event{
		ClickID:    "click-100",
		CampaignID: uuid.New(),
		Type:       "impression",
		CreatedAt:  time.Now(),
	}
	evt2 := &domain.Event{
		ClickID:    "click-100",
		CampaignID: uuid.New(),
		Type:       "click",
		CreatedAt:  time.Now(),
	}

	batchEvents := []*domain.Event{evt1, evt2}

	var preparedTables []string
	var sentTables []string

	connMock := &mockConn{
		prepareBatchFn: func(ctx context.Context, query string) (driver.Batch, error) {
			preparedTables = append(preparedTables, query)
			b := &mockBatch{
				sendFn: func() error {
					sentTables = append(sentTables, query)
					if query == "INSERT INTO clicks" {
						return errors.New("clickhouse connection refused on clicks")
					}
					return nil
				},
			}
			return b, nil
		},
	}

	store := NewClickHouseStore(connMock, 100*time.Millisecond)
	store.SetBatching(1, 0)

	err := store.StoreBatch(context.Background(), batchEvents)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "clickhouse connection refused on clicks")

	assert.True(t, evt1.InsertedToCH, "evt1 (impression) should be marked as inserted")
	assert.False(t, evt2.InsertedToCH, "evt2 (click) should NOT be marked as inserted")

	assert.Contains(t, preparedTables, "INSERT INTO impressions")
	assert.Contains(t, preparedTables, "INSERT INTO clicks")
	assert.Contains(t, sentTables, "INSERT INTO impressions")
	assert.Contains(t, sentTables, "INSERT INTO clicks")

	preparedTables = nil
	sentTables = nil

	connMock.prepareBatchFn = func(ctx context.Context, query string) (driver.Batch, error) {
		preparedTables = append(preparedTables, query)
		b := &mockBatch{
			sendFn: func() error {
				sentTables = append(sentTables, query)
				return nil
			},
		}
		return b, nil
	}

	err = store.StoreBatch(context.Background(), batchEvents)
	assert.NoError(t, err, "Retried batch insertion should succeed")

	assert.True(t, evt1.InsertedToCH)
	assert.True(t, evt2.InsertedToCH)

	assert.NotContains(t, preparedTables, "INSERT INTO impressions", "Should NOT prepare impressions again")
	assert.Contains(t, preparedTables, "INSERT INTO clicks", "Should prepare clicks")
	assert.NotContains(t, sentTables, "INSERT INTO impressions", "Should NOT send impressions again")
	assert.Contains(t, sentTables, "INSERT INTO clicks", "Should send clicks")
}
