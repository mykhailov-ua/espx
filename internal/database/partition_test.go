package database

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockDB struct {
	mock.Mock
}

func (m *MockDB) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	args := m.Called(ctx, sql, arguments)
	return args.Get(0).(pgconn.CommandTag), args.Error(1)
}

func (m *MockDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	callArgs := m.Called(ctx, sql, args)
	if callArgs.Get(0) == nil {
		return nil, callArgs.Error(1)
	}
	return callArgs.Get(0).(pgx.Rows), callArgs.Error(1)
}

func (m *MockDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	callArgs := m.Called(ctx, sql, args)
	return callArgs.Get(0).(pgx.Row)
}

type MockRow struct {
	mock.Mock
	pgx.Row
	val any
}

func (m *MockRow) Scan(dest ...any) error {
	if m.val == nil {
		return pgx.ErrNoRows
	}
	*dest[0].(*int64) = m.val.(int64)
	return nil
}

type MockRows struct {
	mock.Mock
	pgx.Rows
	data []string
	curr int
}

func (m *MockRows) Next() bool {
	return m.curr < len(m.data)
}

func (m *MockRows) Scan(dest ...any) error {
	*dest[0].(*string) = m.data[m.curr]
	m.curr++
	return nil
}

func (m *MockRows) Close()     {}
func (m *MockRows) Err() error { return nil }

func TestPartitionManager_Cleanup(t *testing.T) {
	mockDB := new(MockDB)
	pm := NewPartitionManager(mockDB, 7, 2)

	now := time.Now().UTC()

	testCases := []struct {
		name          string
		existingTabs  []string
		expectedDrops []string
	}{
		{
			name: "Retention and future cleanup",
			existingTabs: []string{
				"events_p2020_01_01",
				"events_p" + now.AddDate(0, 0, -10).Format("2006_01_02"),
				"events_p" + now.Format("2006_01_02"),
				"events_p" + now.AddDate(0, 0, 2).Format("2006_01_02"),
				"events_p" + now.AddDate(0, 0, 10).Format("2006_01_02"),
				"events_p2036_01_01",
			},
			expectedDrops: []string{
				"events_p2020_01_01",
				"events_p" + now.AddDate(0, 0, -10).Format("2006_01_02"),
				"events_p" + now.AddDate(0, 0, 10).Format("2006_01_02"),
				"events_p2036_01_01",
			},
		},
		{
			name: "Strict format and edge cases",
			existingTabs: []string{
				"events_p_broken",
				"events_p",
				"random_table",
				"events_p9999_12_31",
			},
			expectedDrops: []string{
				"events_p9999_12_31",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockDB.ExpectedCalls = nil

			rows := &MockRows{data: tc.existingTabs}
			mockDB.On("Query", mock.Anything, mock.MatchedBy(func(s string) bool {
				return true
			}), mock.Anything).Return(rows, nil).Once()

			mockDB.On("Exec", mock.Anything, mock.MatchedBy(func(s string) bool {
				return true
			}), mock.Anything).Return(pgconn.CommandTag{}, nil)

			mockDB.On("QueryRow", mock.Anything, mock.MatchedBy(func(s string) bool {
				return true
			}), mock.Anything).Return(&MockRow{val: int64(0)})

			err := pm.Run(context.Background())
			assert.NoError(t, err)

			for _, dropped := range tc.expectedDrops {
				mockDB.AssertCalled(t, "Exec", mock.Anything, mock.MatchedBy(func(s string) bool {
					return s == fmt.Sprintf("DROP TABLE IF EXISTS %s;", pgx.Identifier{dropped}.Sanitize())
				}), mock.Anything)
			}
		})
	}
}
