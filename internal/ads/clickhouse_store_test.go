package ads

import (
	"context"
	"errors"
	"strings"
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

func TestClickHouseStore_StoreBatch_DeduplicationTokenFromContext(t *testing.T) {
	evt := &domain.Event{
		ClickID:    "click-100",
		CampaignID: uuid.New(),
		Type:       "impression",
		CreatedAt:  time.Now(),
	}

	var preparedQueries []string

	connMock := &mockConn{
		prepareBatchFn: func(ctx context.Context, query string) (driver.Batch, error) {
			preparedQueries = append(preparedQueries, query)
			return &mockBatch{}, nil
		},
	}

	store := NewClickHouseStore(connMock, 100*time.Millisecond)
	store.SetBatching(1, 0)

	ctx := context.WithValue(context.Background(), domain.DeduplicationTokenKey, "my-custom-test-token")
	err := store.StoreBatch(ctx, []*domain.Event{evt})
	assert.NoError(t, err)

	assert.Len(t, preparedQueries, 1)
	assert.Contains(t, preparedQueries[0], "SETTINGS insert_deduplicate=1")
	assert.Contains(t, preparedQueries[0], "insert_deduplication_token='my-custom-test-token'")
}

func TestClickHouseStore_StoreBatch_DeterministicTokenGeneration(t *testing.T) {
	evt1 := &domain.Event{
		ClickID:    "click-101",
		CampaignID: uuid.New(),
		Type:       "impression",
		CreatedAt:  time.Unix(1600000000, 0),
	}
	evt2 := &domain.Event{
		ClickID:    "click-102",
		CampaignID: uuid.New(),
		Type:       "click",
		CreatedAt:  time.Unix(1600000001, 0),
	}

	var preparedQueries []string

	connMock := &mockConn{
		prepareBatchFn: func(ctx context.Context, query string) (driver.Batch, error) {
			preparedQueries = append(preparedQueries, query)
			return &mockBatch{}, nil
		},
	}

	store := NewClickHouseStore(connMock, 100*time.Millisecond)
	store.SetBatching(1, 0)

	err := store.StoreBatch(context.Background(), []*domain.Event{evt1, evt2})
	assert.NoError(t, err)

	assert.Len(t, preparedQueries, 2)
	q1 := preparedQueries[0]
	q2 := preparedQueries[1]

	assert.Contains(t, q1, "SETTINGS insert_deduplicate=1")
	assert.Contains(t, q2, "SETTINGS insert_deduplicate=1")

	preparedQueries = nil
	err = store.StoreBatch(context.Background(), []*domain.Event{evt1, evt2})
	assert.NoError(t, err)

	assert.Len(t, preparedQueries, 2)
	assert.Equal(t, q1, preparedQueries[0], "Generated query for impressions must be identical")
	assert.Equal(t, q2, preparedQueries[1], "Generated query for clicks must be identical")
}

func TestClickHouseStore_StoreBatch_PartialFailureRetry(t *testing.T) {
	evt1 := &domain.Event{
		ClickID:    "click-201",
		CampaignID: uuid.New(),
		Type:       "impression",
		CreatedAt:  time.Now(),
	}
	evt2 := &domain.Event{
		ClickID:    "click-202",
		CampaignID: uuid.New(),
		Type:       "click",
		CreatedAt:  time.Now(),
	}

	var preparedQueries []string
	var sentQueries []string

	connMock := &mockConn{
		prepareBatchFn: func(ctx context.Context, query string) (driver.Batch, error) {
			preparedQueries = append(preparedQueries, query)
			return &mockBatch{
				sendFn: func() error {
					sentQueries = append(sentQueries, query)
					if strings.Contains(query, "clicks") {
						return errors.New("clickhouse connection refused on clicks")
					}
					return nil
				},
			}, nil
		},
	}

	store := NewClickHouseStore(connMock, 100*time.Millisecond)
	store.SetBatching(1, 0)

	err := store.StoreBatch(context.Background(), []*domain.Event{evt1, evt2})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "clickhouse connection refused on clicks")

	assert.Len(t, preparedQueries, 8, "Should retry 4 times (MaxRetries=3), preparing both impressions and clicks on each attempt")
	assert.Len(t, sentQueries, 8, "Should attempt to send both impressions and clicks on each attempt")
	assert.Contains(t, preparedQueries[0], "impressions")
	assert.Contains(t, preparedQueries[1], "clicks")
}

func TestClickHouseStore_StoreBatch_ContextCancellationDuringBackoff(t *testing.T) {
	evt := &domain.Event{
		ClickID:    "click-301",
		CampaignID: uuid.New(),
		Type:       "impression",
		CreatedAt:  time.Now(),
	}

	ctx, cancel := context.WithCancel(context.Background())

	connMock := &mockConn{
		prepareBatchFn: func(ctx context.Context, query string) (driver.Batch, error) {
			cancel()
			return nil, errors.New("clickhouse connection failed")
		},
	}

	store := NewClickHouseStore(connMock, 100*time.Millisecond)
	store.SetBatching(1, 0)

	err := store.StoreBatch(ctx, []*domain.Event{evt})
	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "context canceled"))
}
