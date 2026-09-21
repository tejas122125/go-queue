package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsEndpoint(t *testing.T) {
	JobsCreated.Inc()
	JobsCompleted.Inc()
	DelayedJobsPromoted.Add(5)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	Handler().ServeHTTP(rec, req)

	resp := rec.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}

	bodyStr := string(body)
	if !strings.Contains(bodyStr, "queue_forge_jobs_created_total") {
		t.Errorf("expected metric queue_forge_jobs_created_total in output")
	}
	if !strings.Contains(bodyStr, "queue_forge_jobs_completed_total") {
		t.Errorf("expected metric queue_forge_jobs_completed_total in output")
	}
	if !strings.Contains(bodyStr, "queue_forge_delayed_jobs_promoted_total") {
		t.Errorf("expected metric queue_forge_delayed_jobs_promoted_total in output")
	}
}

