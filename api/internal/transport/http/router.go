package handler

import (
	"context"
	"net/http"
	"time"

	"github.com/uptrace/bun"

	_ "github.com/barkin94/notification-system/api/docs"
	"github.com/barkin94/notification-system/api/internal/service"
	sharedhandler "github.com/barkin94/notification-system/shared/handler"
)

// Deps holds the dependencies required to build the HTTP router.
type Deps struct {
	Service service.NotificationService
	DB      *bun.DB
}

// NewRouter builds and returns the chi router with all routes registered.
func NewRouter(deps Deps) http.Handler {
	checkers := []sharedhandler.ReadinessChecker{
		{
			Name: "postgresql",
			Check: func(ctx context.Context) error {
				ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
				defer cancel()
				return deps.DB.PingContext(ctx)
			},
		},
	}

	return sharedhandler.NewHandler(sharedhandler.HandlerOpts{
		RegisterRoutesFunc: func(r *sharedhandler.AppRouter) {
			r.Route("/api/v1/notifications", func(r *sharedhandler.AppRouter) {
				r.Post("/", createNotification(deps.Service))
				r.Get("/", listNotifications(deps.Service))
				r.Post("/batch", createBatch(deps.Service))
				r.Get("/{id}", getNotification(deps.Service))
				r.Post("/{id}/cancel", cancelNotification(deps.Service))
			})
		},
		ReadinessChecks: checkers,
		OTelServiceName: "api",
	})
}
