package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"espx/internal/config"
	"espx/internal/database"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManagementAPI_Customers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{
		AdminAPIKey: "test-secret",
	}

	svc := NewService(pool, []redis.UniversalClient{rdb}, nil, cfg)
	defer svc.Close()
	h := NewHandler(svc, cfg, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	custID := uuid.New()
	err := svc.CreateCustomer(context.Background(), custID, "Acme Corp", 150_500_000, "USD")
	require.NoError(t, err)

	err = svc.TopUpBalance(context.Background(), custID, 50_000_000, "idemp-hash-1")
	require.NoError(t, err)

	t.Run("ListCustomers", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/admin/customers?limit=10", nil)
		req.Header.Set("X-Admin-API-Key", "test-secret")
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)
		assert.NotEmpty(t, resp.Header().Get("X-Total-Count"))

		var customers []CustomerDTO
		err := json.NewDecoder(resp.Body).Decode(&customers)
		require.NoError(t, err)
		require.NotEmpty(t, customers)

		var found *CustomerDTO
		for _, c := range customers {
			if c.ID == custID.String() {
				found = &c
				break
			}
		}
		require.NotNil(t, found)
		assert.Equal(t, "Acme Corp", found.Name)
		assert.Equal(t, "200.50", found.Balance)
	})

	t.Run("GetCustomerByID", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/admin/customers/"+custID.String(), nil)
		req.Header.Set("X-Admin-API-Key", "test-secret")
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)

		var cust CustomerDTO
		err := json.NewDecoder(resp.Body).Decode(&cust)
		require.NoError(t, err)
		assert.Equal(t, custID.String(), cust.ID)
		assert.Equal(t, "200.50", cust.Balance)
	})

	t.Run("GetCustomerLedger", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/admin/customers/"+custID.String()+"/ledger", nil)
		req.Header.Set("X-Admin-API-Key", "test-secret")
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Equal(t, "1", resp.Header().Get("X-Total-Count"))

		var ledger []LedgerDTO
		err := json.NewDecoder(resp.Body).Decode(&ledger)
		require.NoError(t, err)
		require.Len(t, ledger, 1)
		assert.Equal(t, "50.00", ledger[0].Amount)
		assert.Equal(t, "TOPUP", ledger[0].Type)
	})

	t.Run("CustomerIsolation_Forbidden", func(t *testing.T) {
		otherCustID := uuid.New()

		req, _ := http.NewRequest("GET", "/admin/customers/"+custID.String(), nil)
		req.Header.Set("X-Admin-API-Key", "test-secret")

		user := AuthenticatedUser{
			UserID:     uuid.New(),
			Role:       "C",
			CustomerID: otherCustID,
		}
		ctx := context.WithValue(req.Context(), UserContextKey, user)

		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req.WithContext(ctx))

		assert.Equal(t, http.StatusForbidden, resp.Code)
	})
}
