package api

import (
	"context"

	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/server"
	"github.com/gorilla/mux"
)

var httpServer *server.Server

// DimensionExtractorAPI manages ...
type DimensionExtractorAPI struct {
	host   string
	healthChan chan bool
	router *mux.Router
}

// CreateDimensionExtractorAPI manages all the routes configured to API
func CreateDimensionExtractorAPI(host, bindAddr string, errorChan chan error, healthChan chan bool) {
	router := mux.NewRouter()
	routes(host, router, healthChan)

	httpServer = server.New(bindAddr, router)
	// Disable this here to allow main to manage graceful shutdown of the entire app.
	httpServer.HandleOSSignals = false

	go func() {
		log.Debug("Starting api...", nil)
		if err := httpServer.ListenAndServe(); err != nil {
			log.ErrorC("api http server returned error", err, nil)
			errorChan <- err
		}
	}()
}

func routes(host string, router *mux.Router, healthChan chan bool) *DimensionExtractorAPI {
	api := DimensionExtractorAPI{host: host, router: router, healthChan: healthChan}

	api.router.Path("/healthcheck").Methods("GET").HandlerFunc(api.healthcheck)
	return &api
}

// Close represents the graceful shutting down of the http server
func Close(ctx context.Context) error {
	if err := httpServer.Shutdown(ctx); err != nil {
		return err
	}
	log.Info("http server gracefully closed ", nil)
	return nil
}
