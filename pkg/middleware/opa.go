package middleware

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/mux"

	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/auth"
	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/config"
	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/logger"
)

// OPAPolicyInput is the JSON structure sent to OPA for policy evaluation.
// The OPA middleware constructs this from JWT claims (caller) and the HTTP request (resource, action).
type OPAPolicyInput struct {
	Input OPAInput `json:"input"`
}

type OPAInput struct {
	Action   string      `json:"action"`
	Resource OPAResource `json:"resource"`
	Caller   OPACaller   `json:"caller"`
}

type OPACaller struct {
	Sub      string `json:"sub"`
	Iss      string `json:"iss,omitempty"`
	TenantID string `json:"tenant_id,omitempty"`
	System   bool   `json:"system,omitempty"`
}

type OPAResource struct {
	Type     string `json:"type"`
	ID       string `json:"id,omitempty"`
	TenantID string `json:"tenant_id,omitempty"`
}

// opaResponse represents the OPA decision response
type opaResponse struct {
	Result bool `json:"result"`
}

// OPAMiddleware creates an HTTP middleware that queries OPA for authorization decisions.
// Fail-closed: returns 503 if OPA is unreachable, 403 if policy denies.
func OPAMiddleware(cfg *config.OPAConfig) func(http.Handler) http.Handler {
	client := &http.Client{
		Timeout: cfg.Timeout,
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()

			policyInput := buildPolicyInput(ctx, r)

			allowed, err := queryOPA(ctx, client, cfg.URL, policyInput)
			if err != nil {
				logger.WithError(ctx, err).Error("OPA query failed")
				http.Error(w, `{"error": "authorization service unavailable"}`, http.StatusServiceUnavailable)
				return
			}

			if !allowed {
				logger.With(ctx,
					"caller_sub", policyInput.Input.Caller.Sub,
					"resource_type", policyInput.Input.Resource.Type,
					"action", policyInput.Input.Action,
				).Warn("OPA denied request")
				http.Error(w, `{"error": "forbidden"}`, http.StatusForbidden)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func buildPolicyInput(ctx context.Context, r *http.Request) *OPAPolicyInput {
	caller := OPACaller{}

	token := auth.GetJWTTokenFromContext(ctx)
	if token != nil {
		if claims, ok := token.Claims.(interface{ GetSubject() (string, error) }); ok {
			sub, err := claims.GetSubject()
			if err != nil {
				logger.WithError(ctx, err).Warn("Failed to extract subject from JWT")
			}
			caller.Sub = sub
		}
		if claims, ok := token.Claims.(interface{ GetIssuer() (string, error) }); ok {
			iss, err := claims.GetIssuer()
			if err != nil {
				logger.WithError(ctx, err).Warn("Failed to extract issuer from JWT")
			}
			caller.Iss = iss
		}
		if mapClaims, ok := token.Claims.(jwt.MapClaims); ok {
			if tid, ok := mapClaims["tenant_id"].(string); ok {
				caller.TenantID = tid
			}
		}
	}

	if caller.Sub == "" {
		caller.Sub = auth.GetUsernameFromContext(ctx)
	}

	resource := OPAResource{
		Type: extractResourceType(r),
		ID:   extractResourceID(r),
	}

	return &OPAPolicyInput{
		Input: OPAInput{
			Caller:   caller,
			Resource: resource,
			Action:   r.Method,
		},
	}
}

func queryOPA(ctx context.Context, client *http.Client, url string, input *OPAPolicyInput) (bool, error) {
	body, err := json.Marshal(input)
	if err != nil {
		return false, fmt.Errorf("failed to marshal OPA input: %w", err)
	}

	start := time.Now()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("failed to create OPA request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return false, fmt.Errorf("OPA request failed: %w", err)
	}
	defer resp.Body.Close()

	logger.With(ctx, "opa_latency_ms", time.Since(start).Milliseconds()).Debug("OPA query completed")

	if resp.StatusCode != http.StatusOK {
		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return false, fmt.Errorf("OPA returned status %d (body unreadable: %w)", resp.StatusCode, readErr)
		}
		return false, fmt.Errorf("OPA returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var opaResp opaResponse
	if err := json.NewDecoder(resp.Body).Decode(&opaResp); err != nil {
		return false, fmt.Errorf("failed to decode OPA response: %w", err)
	}

	return opaResp.Result, nil
}

// extractResourceType derives the resource type from the URL path.
// For /api/hyperfleet/v1/clusters/abc-123, returns "clusters".
func extractResourceType(r *http.Request) string {
	route := mux.CurrentRoute(r)
	if route == nil {
		return extractResourceTypeFromPath(r.URL.Path)
	}

	pathTemplate, err := route.GetPathTemplate()
	if err != nil {
		return extractResourceTypeFromPath(r.URL.Path)
	}

	// Template looks like /api/hyperfleet/v1/{resource_type} or /api/hyperfleet/v1/{resource_type}/{id}
	parts := strings.Split(strings.TrimPrefix(pathTemplate, "/api/hyperfleet/v1/"), "/")
	if len(parts) > 0 {
		return parts[0]
	}
	return ""
}

func extractResourceTypeFromPath(path string) string {
	const prefix = "/api/hyperfleet/v1/"
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	remainder := strings.TrimPrefix(path, prefix)
	parts := strings.SplitN(remainder, "/", 2)
	if len(parts) > 0 {
		return parts[0]
	}
	return ""
}

// extractResourceID pulls the resource ID from mux vars if present.
func extractResourceID(r *http.Request) string {
	vars := mux.Vars(r)
	if id, ok := vars["id"]; ok {
		return id
	}
	return ""
}
