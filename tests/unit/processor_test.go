package unit

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/repository"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type MockQuerier struct {
	repository.Querier
	mock.Mock
	mu      sync.Mutex
	flushes [][]ads.Event
}

func (m *MockQuerier) InsertEventsBatch(ctx context.Context, arg repository.InsertEventsBatchParams) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var batch []ads.Event
	for i := range arg.ClickIds {
		batch = append(batch, ads.Event{
			ClickID:    arg.ClickIds[i],
			CampaignID: uuid.UUID(arg.CampaignIds[i].Bytes),
			Type:       arg.EventTypes[i],
			Payload:    arg.Payloads[i],
			IP:         arg.IpAddresses[i],
			UA:         arg.UserAgents[i],
		})
	}

	// Copy the batch to avoid issues with underlying array reuse in Processor
	batchCopy := make([]ads.Event, len(batch))
	copy(batchCopy, batch)
	m.flushes = append(m.flushes, batchCopy)
	return nil
}

func (m *MockQuerier) ListCampaignIDs(ctx context.Context) ([]pgtype.UUID, error) {
	return nil, nil
}

func TestProcessor_BufferOverflow(t *testing.T) {
	mockRepo := &MockQuerier{}
	proc := ads.NewProcessor(mockRepo, 5, 1, 100*time.Millisecond, 1*time.Second)
	proc.Start(context.Background())

	campaignID := uuid.New()
	// New buffer size is batchSize * (maxWorkers + 1) = 5 * 2 = 10
	for i := 0; i < 11; i++ {
		err := proc.Process(ads.Event{ClickID: uuid.New().String(), CampaignID: campaignID})
		if i < 10 {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
			assert.Equal(t, ads.ErrBufferFull, err)
		}
	}
}

func TestProcessor_BatchFlushing(t *testing.T) {
	mockRepo := &MockQuerier{}
	// Batch size 2, 1 worker, long flush interval to avoid random flushes
	proc := ads.NewProcessor(mockRepo, 2, 1, 10*time.Second, 1*time.Second)
	proc.Start(context.Background())

	campaignID := uuid.New()

	// Send 3 events
	// Event 1 & 2 should trigger a flush immediately (batch size 2)
	// Event 3 should stay in buffer until Close()
	for i := 0; i < 3; i++ {
		err := proc.Process(ads.Event{ClickID: uuid.New().String(), CampaignID: campaignID})
		assert.NoError(t, err)
	}

	// Give a small moment for the first batch to be processed by the worker
	time.Sleep(50 * time.Millisecond)

	proc.Close()
	proc.Wait()

	mockRepo.mu.Lock()
	count := len(mockRepo.flushes)
	flushes := mockRepo.flushes
	mockRepo.mu.Unlock()

	require.Equal(t, 2, count, "Should have 2 separate flushes")
	assert.Equal(t, 2, len(flushes[0]), "First flush should contain 2 events")
	assert.Equal(t, 1, len(flushes[1]), "Second flush (drain) should contain 1 event")
}
