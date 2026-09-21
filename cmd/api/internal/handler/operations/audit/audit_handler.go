package audit

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"queue-forge/cmd/api/internal/apierr"
	"queue-forge/cmd/api/internal/handler"
	dbgen "queue-forge/internal/db/gen"
)

type AuditRecord struct {
	ID            string                 `json:"id"`
	ActorID       string                 `json:"actorId"`
	Action        string                 `json:"action"`
	TargetType    string                 `json:"targetType"`
	TargetID      string                 `json:"targetId"`
	Reason        string                 `json:"reason"`
	Outcome       string                 `json:"outcome"`
	RequestID     *string                `json:"requestId"`
	ChangeSummary map[string]interface{} `json:"changeSummary"`
	CreatedAt     string                 `json:"createdAt"`
}

type AuditCursor struct {
	CreatedAt string `json:"createdAt"`
	ID        string `json:"id"`
}

type AuditPage struct {
	Items      []AuditRecord `json:"items"`
	NextCursor *string       `json:"nextCursor"`
}

type AuditHandler struct {
	pool    *pgxpool.Pool
	queries *dbgen.Queries
}

func New(pool *pgxpool.Pool) *AuditHandler {
	return &AuditHandler{
		pool:    pool,
		queries: dbgen.New(pool),
	}
}

func (h *AuditHandler) List(w http.ResponseWriter, r *http.Request) {
	limit := 20
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 100 {
			limit = parsed
		}
	}

	actionFilter := r.URL.Query().Get("action")
	actorIDFilter := r.URL.Query().Get("actorId")
	targetTypeFilter := r.URL.Query().Get("targetType")
	targetIDFilter := r.URL.Query().Get("targetId")
	fromFilter := r.URL.Query().Get("from")
	toFilter := r.URL.Query().Get("to")

	cursorStr := r.URL.Query().Get("cursor")
	var cursor *AuditCursor
	if cursorStr != "" {
		var c AuditCursor
		if err := handler.DecodeCursor(cursorStr, &c); err != nil {
			handler.WriteError(w, apierr.InvalidCursor())
			return
		}
		cursor = &c
	}

	query := `SELECT id, actor_id, action, target_type, target_id, reason, outcome, request_id, change_summary, created_at FROM audit_logs WHERE 1=1`
	var args []interface{}
	argIdx := 1

	if actionFilter != "" {
		query += fmt.Sprintf(" AND action = $%d", argIdx)
		args = append(args, actionFilter)
		argIdx++
	}
	if actorIDFilter != "" {
		query += fmt.Sprintf(" AND actor_id = $%d", argIdx)
		args = append(args, actorIDFilter)
		argIdx++
	}
	if targetTypeFilter != "" {
		query += fmt.Sprintf(" AND target_type = $%d", argIdx)
		args = append(args, targetTypeFilter)
		argIdx++
	}
	if targetIDFilter != "" {
		query += fmt.Sprintf(" AND target_id = $%d", argIdx)
		args = append(args, targetIDFilter)
		argIdx++
	}
	if fromFilter != "" {
		if t, err := time.Parse(time.RFC3339, fromFilter); err == nil {
			query += fmt.Sprintf(" AND created_at >= $%d", argIdx)
			args = append(args, t)
			argIdx++
		}
	}
	if toFilter != "" {
		if t, err := time.Parse(time.RFC3339, toFilter); err == nil {
			query += fmt.Sprintf(" AND created_at <= $%d", argIdx)
			args = append(args, t)
			argIdx++
		}
	}

	if cursor != nil {
		cursorTime, err := time.Parse(time.RFC3339Nano, cursor.CreatedAt)
		if err != nil {
			cursorTime, _ = time.Parse(time.RFC3339, cursor.CreatedAt)
		}
		if cursorUUID, err := uuid.Parse(cursor.ID); err == nil {
			query += fmt.Sprintf(" AND (created_at < $%d OR (created_at = $%d AND id < $%d))", argIdx, argIdx, argIdx+1)
			args = append(args, cursorTime, cursorUUID)
			argIdx += 2
		}
	}

	query += fmt.Sprintf(" ORDER BY created_at DESC, id DESC LIMIT $%d", argIdx)
	args = append(args, limit+1)

	rows, err := h.pool.Query(r.Context(), query, args...)
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	defer rows.Close()

	var items []AuditRecord
	for rows.Next() {
		var id pgtype.UUID
		var actorID string
		var action string
		var targetType string
		var targetID string
		var reason string
		var outcome string
		var requestID pgtype.Text
		var changeSummary []byte
		var createdAt pgtype.Timestamp

		if err := rows.Scan(&id, &actorID, &action, &targetType, &targetID, &reason, &outcome, &requestID, &changeSummary, &createdAt); err != nil {
			handler.WriteError(w, err)
			return
		}

		var reqIDPtr *string
		if requestID.Valid {
			reqIDPtr = &requestID.String
		}

		var summary map[string]interface{}
		_ = json.Unmarshal(changeSummary, &summary)

		items = append(items, AuditRecord{
			ID:            uuidToString(id),
			ActorID:       actorID,
			Action:        action,
			TargetType:    targetType,
			TargetID:      targetID,
			Reason:        reason,
			Outcome:       outcome,
			RequestID:     reqIDPtr,
			ChangeSummary: summary,
			CreatedAt:     createdAt.Time.Format(time.RFC3339Nano),
		})
	}

	hasMore := len(items) > limit
	page := items
	if hasMore {
		page = items[:limit]
	}

	var nextCursor *string
	if hasMore && len(page) > 0 {
		last := page[len(page)-1]
		encoded, err := handler.EncodeCursor(AuditCursor{
			CreatedAt: last.CreatedAt,
			ID:        last.ID,
		})
		if err == nil {
			nextCursor = &encoded
		}
	}

	handler.WriteJSON(w, http.StatusOK, AuditPage{
		Items:      page,
		NextCursor: nextCursor,
	})
}

func uuidToString(u pgtype.UUID) string {
	if !u.Valid {
		return ""
	}
	b := u.Bytes
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

