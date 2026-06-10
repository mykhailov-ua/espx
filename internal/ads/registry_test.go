package ads

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"espx/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegistry_LockFreeReadsStress(t *testing.T) {
	mock := &MockRepo{}
	r := newTestRegistry(t, mock)

	id1 := uuid.New()
	customerID1 := uuid.New()
	r.Add(id1, customerID1, nil, "", domain.PacingModeAsap, 1000, "UTC", 0, 0, nil)

	var wg sync.WaitGroup

	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = r.Exists(id1)
				_, _ = r.GetCustomerID(id1)
				_, _ = r.GetCampaign(id1)
			}
		}()
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				r.Add(uuid.New(), uuid.New(), nil, "", domain.PacingModeAsap, 1000, "UTC", 0, 0, nil)
			}
		}()
	}

	wg.Wait()
	assert.True(t, r.Exists(id1))
	cust, ok := r.GetCustomerID(id1)
	assert.True(t, ok)
	assert.Equal(t, customerID1, cust)
}

func TestRegistry_FileReplicationAndFailover(t *testing.T) {
	replicaPath := filepath.Join(t.TempDir(), "campaigns_replica.json")

	id1 := uuid.New()
	id2 := uuid.New()
	mockSuccess := &MockRepo{
		ids: []pgtype.UUID{
			{Bytes: id1, Valid: true},
			{Bytes: id2, Valid: true},
		},
	}

	r1 := NewRegistry(mockSuccess)
	r1.SetReplicaPath(replicaPath)

	count, err := r1.Sync(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, count)
	assert.True(t, r1.Exists(id1))
	assert.True(t, r1.Exists(id2))

	_, err = os.Stat(replicaPath)
	assert.NoError(t, err, "replica file must exist on disk")

	mockFail := &MockRepo{
		err: errors.New("database is completely offline"),
	}

	r2 := NewRegistry(mockFail)
	r2.SetReplicaPath(replicaPath)

	count2, err2 := r2.Sync(context.Background())
	require.NoError(t, err2, "Sync must not fail; it should fallback to loading from the replica file")
	assert.Equal(t, 2, count2, "loaded campaign count should match the replica")
	assert.True(t, r2.Exists(id1))
	assert.True(t, r2.Exists(id2))
}
