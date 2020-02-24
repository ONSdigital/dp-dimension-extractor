package api

import (
	"context"

	"github.com/ONSdigital/go-ns/server"
	"github.com/ONSdigital/log.go/log"
	"github.com/gorilla/mux"
)

var httpServer *server.Server

// DimensionExtractorAPI manages ...
type DimensionExtractorAPI struct {
	host   string
	router *mux.Router
}

// CreateDimensionExtractorAPI manages all the routes configured to API
func CreateDimensionExtractorAPI(host, bindAddr string, errorChan chan error) {
	ctx := context.Background()
	router := mux.NewRouter()
	routes(host, router)

	httpServer = server.New(bindAddr, router)
	// Disable this here to allow main to manage graceful shutdown of the entire app.
	httpServer.HandleOSSignals = false

	go func() {
		log.Event(ctx, "Starting api...", log.INFO)
		if err := httpServer.ListenAndServe(); err != nil {
			log.Event(ctx, "api http server returned error", log.ERROR, log.Error(err))
			errorChan <- err
		}
	}()
}

func routes(host string, router *mux.Router) *DimensionExtractorAPI {
	api := DimensionExtractorAPI{host: host, router: router}

	api.router.Path("/healthcheck").Methods("GET").HandlerFunc(healthCheck)

	return &api
}

// Close represents the graceful shutting down of the http server
func Close(ctx context.Context) error {
	if err := httpServer.Shutdown(ctx); err != nil {
		return err
	}
	log.Event(ctx, "http server gracefully closed ", log.INFO)
	return nil
}
