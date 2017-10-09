package api

import (
	"net/http"

	"github.com/ONSdigital/go-ns/log"
)

// HealthCheck returns the health of the application.
func healthCheck(w http.ResponseWriter, r *http.Request) {
	log.Debug("Healthcheck endpoint.", nil)
	w.WriteHeader(http.StatusOK)
}
