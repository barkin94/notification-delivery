package handler

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	sharedErrors "github.com/barkin94/notification-system/shared/genericerrors"
	sharedotel "github.com/barkin94/notification-system/shared/otel"
)

type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

type errorResponse struct {
	Error ErrorBody `json:"error"`
}

func errHandler(h AppHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := h(w, r); err != nil {
			slog.ErrorContext(r.Context(), "error", "error", err.Error())

			sharedotel.RecordError(r.Context(), err)

			var notFoundErr *sharedErrors.NotFoundError
			if errors.As(err, &notFoundErr) {
				writeError(w, http.StatusNotFound, "NOT_FOUND", notFoundErr.Message, nil)
				return
			}

			var conflictErr *sharedErrors.ConflictError
			if errors.As(err, &conflictErr) {
				writeError(w, http.StatusConflict, "CONFLICT", conflictErr.Message, nil)
				return
			}

			var domainErr sharedErrors.DomainError
			if errors.As(err, &domainErr) {
				writeError(w, http.StatusUnprocessableEntity, domainErr.Code, domainErr.Message, nil)
				return
			}

			var valErr *ValidationError
			if errors.As(err, &valErr) {
				writeError(w, http.StatusUnprocessableEntity, "VALIDATION_ERROR", "validation failed", valErr.Fields)
				return
			}

			var jsonSyntax *json.SyntaxError
			var jsonType *json.UnmarshalTypeError
			if errors.As(err, &jsonSyntax) || errors.As(err, &jsonType) || err == io.EOF || err == io.ErrUnexpectedEOF {
				writeError(w, http.StatusUnprocessableEntity, "VALIDATION_ERROR", "invalid request body", nil)
				return
			}

			writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error(), nil)
		}
	}
}

func writeError(w http.ResponseWriter, status int, code, message string, details any) {
	WriteJSON(w, status, errorResponse{Error: ErrorBody{Code: code, Message: message, Details: details}})
}
