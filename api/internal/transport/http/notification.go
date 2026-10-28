package handler

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/barkin94/notification-system/api/internal/db"
	"github.com/barkin94/notification-system/api/internal/domain/notification"
	"github.com/barkin94/notification-system/api/internal/service"
	sharedhandler "github.com/barkin94/notification-system/shared/handler"
)

// --- handler constructors ---

// createNotification godoc
// @Summary     Create a notification
// @Tags        notifications
// @Accept      json
// @Produce     json
// @Param       body body createRequest true "Notification payload"
// @Success     201 {object} notificationResponse
// @Failure     400 {object} sharedhandler.ErrorBody
// @Failure     500 {object} sharedhandler.ErrorBody
// @Router      /notifications [post]
func createNotification(svc service.NotificationService) sharedhandler.AppHandler {
	return func(w http.ResponseWriter, r *http.Request) error {
		req, err := sharedhandler.DecodeBody[createRequest](r)
		if err != nil {
			return err
		}

		n, err := req.ToNotification()
		if err != nil {
			return err
		}

		result, err := svc.Create(r.Context(), n)
		if err != nil {
			return err
		}

		sharedhandler.WriteJSON(w, http.StatusCreated, toNotificationResponse(result))
		return nil
	}
}

// getNotification godoc
// @Summary     Get a notification
// @Tags        notifications
// @Produce     json
// @Param       id path string true "Notification ID (UUID)"
// @Success     200 {object} notificationResponse
// @Failure     400 {object} sharedhandler.ErrorBody
// @Failure     404 {object} sharedhandler.ErrorBody
// @Failure     500 {object} sharedhandler.ErrorBody
// @Router      /notifications/{id} [get]
func getNotification(svc service.NotificationService) sharedhandler.AppHandler {
	return func(w http.ResponseWriter, r *http.Request) error {
		id, err := uuid.Parse(chi.URLParam(r, "id"))
		if err != nil {
			return sharedhandler.NewValidationError("id", "must be a valid UUID")
		}

		n, err := svc.GetByID(r.Context(), id)
		if err != nil {
			return err
		}

		sharedhandler.WriteJSON(w, http.StatusOK, toNotificationResponse(n))
		return nil
	}
}

// listNotifications godoc
// @Summary     List notifications
// @Tags        notifications
// @Produce     json
// @Param       status    query string false "Filter by status"
// @Param       channel   query string false "Filter by channel"
// @Param       batch_id  query string false "Filter by batch ID (UUID)"
// @Param       date_from query string false "Filter from date (RFC3339)"
// @Param       date_to   query string false "Filter to date (RFC3339)"
// @Param       page      query int    false "Page number for offset pagination (default 1, ignored when cursor is set)"
// @Param       page_size query int    false "Page size (default 20, max 100)"
// @Param       cursor    query string false "ID of the last item on the current page; when set, returns items strictly before it (keyset pagination, takes precedence over page)"
// @Success     200 {object} listResponse
// @Failure     400 {object} sharedhandler.ErrorBody
// @Failure     500 {object} sharedhandler.ErrorBody
// @Router      /notifications [get]
func listNotifications(svc service.NotificationService) sharedhandler.AppHandler {
	return func(w http.ResponseWriter, r *http.Request) error {
		q := r.URL.Query()

		pageSize := intParam(q.Get("page_size"), 20)
		if pageSize > 100 {
			pageSize = 100
		}

		var batchID *uuid.UUID
		if s := q.Get("batch_id"); s != "" {
			if id, err := uuid.Parse(s); err == nil {
				batchID = &id
			}
		}
		var dateFrom, dateTo *time.Time
		if s := q.Get("date_from"); s != "" {
			if t, err := time.Parse(time.RFC3339, s); err == nil {
				dateFrom = &t
			}
		}
		if s := q.Get("date_to"); s != "" {
			if t, err := time.Parse(time.RFC3339, s); err == nil {
				dateTo = &t
			}
		}

		f := db.ListFilter{
			Status:   q.Get("status"),
			Channel:  q.Get("channel"),
			BatchID:  batchID,
			DateFrom: dateFrom,
			DateTo:   dateTo,
			Sort:     q.Get("sort"),
			Order:    q.Get("order"),
			Page:     intParam(q.Get("page"), 1),
			PageSize: pageSize,
		}

		if cursorStr := q.Get("cursor"); cursorStr != "" {
			cursorID, err := decodeCursor(cursorStr)
			if err != nil {
				return sharedhandler.NewValidationError("cursor", "invalid")
			}
			f.CursorID = cursorID
		}

		ns, total, nextCursor, err := svc.List(r.Context(), f)
		if err != nil {
			return err
		}
		totalPages := (total + pageSize - 1) / pageSize
		sharedhandler.WriteJSON(w, http.StatusOK, listResponse{
			Data: toNotificationResponses(ns),
			Pagination: paginationMeta{
				Page:       f.Page,
				PageSize:   pageSize,
				Total:      total,
				TotalPages: totalPages,
				NextCursor: encodeCursor(nextCursor),
			},
		})
		return nil
	}
}

func decodeCursor(s string) (*uuid.UUID, error) {
	id, err := uuid.Parse(s)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

func encodeCursor(id *uuid.UUID) *string {
	if id == nil {
		return nil
	}
	s := id.String()
	return &s
}

// cancelNotification godoc
// @Summary     Cancel a notification
// @Tags        notifications
// @Produce     json
// @Param       id path string true "Notification ID (UUID)"
// @Success     200 {object} cancelResponse
// @Failure     400 {object} sharedhandler.ErrorBody
// @Failure     404 {object} sharedhandler.ErrorBody
// @Failure     409 {object} sharedhandler.ErrorBody
// @Failure     500 {object} sharedhandler.ErrorBody
// @Router      /notifications/{id}/cancel [post]
func cancelNotification(svc service.NotificationService) sharedhandler.AppHandler {
	return func(w http.ResponseWriter, r *http.Request) error {
		id, err := uuid.Parse(chi.URLParam(r, "id"))
		if err != nil {
			return sharedhandler.NewValidationError("id", "must be a valid UUID")
		}

		n, err := svc.Cancel(r.Context(), id)
		if err != nil {
			return err
		}

		sharedhandler.WriteJSON(w, http.StatusOK, cancelResponse{
			ID:        n.ID.String(),
			Status:    n.Status,
			UpdatedAt: n.UpdatedAt.Format(time.RFC3339),
		})
		return nil
	}
}

// createBatch godoc
// @Summary     Create a batch of notifications
// @Tags        notifications
// @Accept      json
// @Produce     json
// @Param       body body batchRequest true "Batch payload (max 1000 items)"
// @Success     207 {object} batchResponse
// @Failure     400 {object} sharedhandler.ErrorBody
// @Failure     500 {object} sharedhandler.ErrorBody
// @Router      /notifications/batch [post]
func createBatch(svc service.NotificationService) sharedhandler.AppHandler {
	return func(w http.ResponseWriter, r *http.Request) error {
		req, err := sharedhandler.DecodeBody[batchRequest](r)
		if err != nil {
			return err
		}
		if len(req.Notifications) == 0 {
			return sharedhandler.NewValidationError("notifications", "must not be empty")
		}
		if len(req.Notifications) > 1000 {
			return sharedhandler.NewValidationError("notifications", "max 1000 items")
		}

		type validItem struct {
			idx int
			n   notification.Notification
		}

		var valid []validItem
		itemResults := make([]batchItemResult, 0, len(req.Notifications))

		for i, item := range req.Notifications {
			n, err := item.ToNotification()
			if err != nil {
				msg := err.Error()
				itemResults = append(itemResults, batchItemResult{
					Index:  i,
					Status: "rejected",
					Error:  &sharedhandler.ErrorBody{Code: "VALIDATION_ERROR", Message: msg},
				})
				continue
			}
			valid = append(valid, validItem{idx: i, n: n})
		}

		ns := make([]notification.Notification, len(valid))
		for j, v := range valid {
			ns[j] = v.n
		}

		batchID, results, err := svc.CreateBatch(r.Context(), ns)
		if err != nil {
			return err
		}

		accepted := 0
		for j, res := range results {
			item := batchItemResult{Index: valid[j].idx, Status: res.Status}
			if res.ID != nil {
				id := res.ID.String()
				item.ID = &id
				accepted++
			}
			if res.Error != nil {
				item.Error = &sharedhandler.ErrorBody{Code: "INTERNAL_ERROR", Message: *res.Error}
			}
			itemResults = append(itemResults, item)
		}

		sharedhandler.WriteJSON(w, http.StatusMultiStatus, batchResponse{
			BatchID:  batchID.String(),
			Total:    len(req.Notifications),
			Accepted: accepted,
			Rejected: len(req.Notifications) - accepted,
			Results:  itemResults,
		})
		return nil
	}
}

func intParam(s string, def int) int {
	if v, err := strconv.Atoi(s); err == nil && v > 0 {
		return v
	}
	return def
}
