package intakehttp_test

import (
	"errors"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	intakehttp "fleet-telemetry/internal/telemetry/intake/http"
)

var authNow = time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)

func signToken(t *testing.T, secret []byte, sub string, exp time.Time, method jwt.SigningMethod) string {
	t.Helper()
	claims := jwt.RegisteredClaims{
		Subject:   sub,
		ExpiresAt: jwt.NewNumericDate(exp),
	}
	token, err := jwt.NewWithClaims(method, claims).SignedString(secret)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return token
}

func TestAuthenticatorDeviceID(t *testing.T) {
	secret := []byte("dev-secret")
	auth := intakehttp.NewAuthenticator(secret, func() time.Time { return authNow })

	tests := []struct {
		name       string
		header     string
		wantDevice string
		wantErr    error
	}{
		{
			name:       "valid token",
			header:     "Bearer " + signToken(t, secret, "dev-1", authNow.Add(time.Hour), jwt.SigningMethodHS256),
			wantDevice: "dev-1",
		},
		{
			name:    "expired token",
			header:  "Bearer " + signToken(t, secret, "dev-1", authNow.Add(-time.Minute), jwt.SigningMethodHS256),
			wantErr: intakehttp.ErrInvalidToken,
		},
		{
			name:    "signed with the wrong secret",
			header:  "Bearer " + signToken(t, []byte("other-secret"), "dev-1", authNow.Add(time.Hour), jwt.SigningMethodHS256),
			wantErr: intakehttp.ErrInvalidToken,
		},
		{
			name:    "signed with another algorithm",
			header:  "Bearer " + signToken(t, secret, "dev-1", authNow.Add(time.Hour), jwt.SigningMethodHS512),
			wantErr: intakehttp.ErrInvalidToken,
		},
		{
			name:    "malformed token",
			header:  "Bearer not-a-jwt",
			wantErr: intakehttp.ErrInvalidToken,
		},
		{
			name:    "empty subject",
			header:  "Bearer " + signToken(t, secret, "", authNow.Add(time.Hour), jwt.SigningMethodHS256),
			wantErr: intakehttp.ErrInvalidToken,
		},
		{
			name:    "missing header",
			header:  "",
			wantErr: intakehttp.ErrMissingToken,
		},
		{
			name:    "wrong scheme",
			header:  "Basic " + signToken(t, secret, "dev-1", authNow.Add(time.Hour), jwt.SigningMethodHS256),
			wantErr: intakehttp.ErrMissingToken,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			device, err := auth.DeviceID(tt.header)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("DeviceID() error = %v, want %v", err, tt.wantErr)
				}
				if device != "" {
					t.Fatalf("DeviceID() = %q on error, want empty", device)
				}
				return
			}
			if err != nil {
				t.Fatalf("DeviceID() error = %v, want nil", err)
			}
			if device != tt.wantDevice {
				t.Fatalf("DeviceID() = %q, want %q", device, tt.wantDevice)
			}
		})
	}
}
