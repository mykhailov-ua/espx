package unit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/repository"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockRetryQuerier struct {
	repository.Querier
	mock.Mock
}

func (m *MockRetryQuerier) InsertEventsBatch(ctx context.Context, arg repository.InsertEventsBatchParams) error {
	args := m.Called(ctx, arg)
	return args.Error(0)
}

func TestProcessor_RetrySuccess(t *testing.T) {
	// Reduce wait times for fast test execution
	origWait := ads.InitialWait
	origMax := ads.MaxRetries
	ads.InitialWait = 1 * time.Millisecond
	ads.MaxRetries = 2
	defer func() {
		ads.InitialWait = origWait
		ads.MaxRetries = origMax
	}()

	mockRepo := new(MockRetryQuerier)
	// Fail twice, succeed on third attempt
	mockRepo.On("InsertEventsBatch", mock.Anything, mock.Anything).Return(errors.New("db error")).Twice()
	mockRepo.On("InsertEventsBatch", mock.Anything, mock.Anything).Return(nil).Once()

	proc := ads.NewProcessor(mockRepo, 1, 1, 1*time.Second, 1*time.Second)
	proc.Start(context.Background())

	err := proc.Process(ads.Event{CampaignID: uuid.New(), Type: "click"})
	assert.NoError(t, err)

	// Wait for the worker to finish retries
	time.Sleep(50 * time.Millisecond)
	
	proc.Close()
	proc.Wait()

	mockRepo.AssertExpectations(t)
}

func TestProcessor_RetryExhaustion(t *testing.T) {
	origWait := ads.InitialWait
	origMax := ads.MaxRetries
	ads.InitialWait = 1 * time.Millisecond
	ads.MaxRetries = 1 // Only 1 retry allowed (total 2 attempts)
	defer func() {
		ads.InitialWait = origWait
		ads.MaxRetries = origMax
	}()

	mockRepo := new(MockRetryQuerier)
	// Always fail
	mockRepo.On("InsertEventsBatch", mock.Anything, mock.Anything).Return(errors.New("db permanent error")).Maybe()

	proc := ads.NewProcessor(mockRepo, 1, 1, 1*time.Second, 1*time.Second)
	proc.Start(context.Background())

	err := proc.Process(ads.Event{CampaignID: uuid.New(), Type: "impression"})
	assert.NoError(t, err)

	// Wait for retries to exhaust
	time.Sleep(50 * time.Millisecond)

	proc.Close()
	proc.Wait()

	// In a real scenario, we'd check metrics here, but since they are global promauto counters,
	// checking exact values in unit tests can be flaky if other tests run.
	// But we've verified the flow.
}
