package api

import (
	"net/http"

	"github.com/ONSdigital/log.go/log"
)

// HealthCheck returns the health of the application.
func healthCheck(w http.ResponseWriter, r *http.Request) {
	log.Event(nil, "Healthcheck endpoint.", log.INFO)
	w.WriteHeader(http.StatusOK)
}
