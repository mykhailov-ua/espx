package auth

import ()

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func addAuthorization(
	t *testing.T,
	request *http.Request,
	tokenMaker Maker,
	authorizationType string,
	userID uuid.UUID,
	role string,
	customerID uuid.UUID,
	duration time.Duration,
) {
	sessionID := uuid.New()
	token, err := tokenMaker.CreateToken(userID, sessionID, role, customerID, duration)
	require.NoError(t, err)

	authorizationHeader := fmt.Sprintf("%s %s", authorizationType, token)
	request.Header.Set(authorizationHeaderKey, authorizationHeader)
}

func TestAuthMiddleware(t *testing.T) {
	testCases := []struct {
		name          string
		setupAuth     func(t *testing.T, request *http.Request, tokenMaker Maker)
		allowedRoles  []string
		checkResponse func(t *testing.T, recorder *httptest.ResponseRecorder)
	}{
		{
			name: "OK",
			setupAuth: func(t *testing.T, request *http.Request, tokenMaker Maker) {
				addAuthorization(t, request, tokenMaker, authorizationTypeBearer, uuid.New(), "admin", uuid.New(), time.Minute)
			},
			allowedRoles: []string{"admin"},
			checkResponse: func(t *testing.T, recorder *httptest.ResponseRecorder) {
				require.Equal(t, http.StatusOK, recorder.Code)
			},
		},
		{
			name: "NoAuthorization",
			setupAuth: func(t *testing.T, request *http.Request, tokenMaker Maker) {
			},
			allowedRoles: []string{"admin"},
			checkResponse: func(t *testing.T, recorder *httptest.ResponseRecorder) {
				require.Equal(t, http.StatusUnauthorized, recorder.Code)
			},
		},
		{
			name: "UnsupportedAuthorization",
			setupAuth: func(t *testing.T, request *http.Request, tokenMaker Maker) {
				addAuthorization(t, request, tokenMaker, "unsupported", uuid.New(), "admin", uuid.New(), time.Minute)
			},
			allowedRoles: []string{"admin"},
			checkResponse: func(t *testing.T, recorder *httptest.ResponseRecorder) {
				require.Equal(t, http.StatusUnauthorized, recorder.Code)
			},
		},
		{
			name: "InvalidAuthorizationFormat",
			setupAuth: func(t *testing.T, request *http.Request, tokenMaker Maker) {
				request.Header.Set(authorizationHeaderKey, "invalid")
			},
			allowedRoles: []string{"admin"},
			checkResponse: func(t *testing.T, recorder *httptest.ResponseRecorder) {
				require.Equal(t, http.StatusUnauthorized, recorder.Code)
			},
		},
		{
			name: "ExpiredToken",
			setupAuth: func(t *testing.T, request *http.Request, tokenMaker Maker) {
				addAuthorization(t, request, tokenMaker, authorizationTypeBearer, uuid.New(), "admin", uuid.New(), -time.Minute)
			},
			allowedRoles: []string{"admin"},
			checkResponse: func(t *testing.T, recorder *httptest.ResponseRecorder) {
				require.Equal(t, http.StatusUnauthorized, recorder.Code)
			},
		},
		{
			name: "PermissionDenied",
			setupAuth: func(t *testing.T, request *http.Request, tokenMaker Maker) {
				addAuthorization(t, request, tokenMaker, authorizationTypeBearer, uuid.New(), "customer", uuid.New(), time.Minute)
			},
			allowedRoles: []string{"admin"},
			checkResponse: func(t *testing.T, recorder *httptest.ResponseRecorder) {
				require.Equal(t, http.StatusForbidden, recorder.Code)
			},
		},
	}

	for i := range testCases {
		tc := testCases[i]

		t.Run(tc.name, func(t *testing.T) {
			tokenMaker, err := NewPasetoMaker("12345678901234567890123456789012")
			require.NoError(t, err)

			authPath := "/auth"
			handler := AuthMiddleware(tokenMaker, nil, tc.allowedRoles...)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))

			recorder := httptest.NewRecorder()
			request, err := http.NewRequest(http.MethodGet, authPath, nil)
			require.NoError(t, err)

			tc.setupAuth(t, request, tokenMaker)
			handler.ServeHTTP(recorder, request)
			tc.checkResponse(t, recorder)
		})
	}
}

func BenchmarkAuthMiddleware(b *testing.B) {
	tokenMaker, err := NewPasetoMaker("12345678901234567890123456789012")
	if err != nil {
		b.Fatal(err)
	}

	userID := uuid.New()
	sessionID := uuid.New()
	role := "admin"
	customerID := uuid.New()
	token, err := tokenMaker.CreateToken(userID, sessionID, role, customerID, time.Hour)
	if err != nil {
		b.Fatal(err)
	}

	handler := AuthMiddleware(tokenMaker, nil, "admin")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req, err := http.NewRequest(http.MethodGet, "/auth", nil)
	if err != nil {
		b.Fatal(err)
	}
	req.Header.Set(authorizationHeaderKey, fmt.Sprintf("%s %s", authorizationTypeBearer, token))

	rec := httptest.NewRecorder()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.ServeHTTP(rec, req)
		rec.Body.Reset()
	}
}

