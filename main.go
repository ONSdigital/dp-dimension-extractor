package main

import (
	"errors"
	"os"
	"strconv"

	"fmt"
	"os/signal"
	"syscall"

	"github.com/ONSdigital/dp-api-clients-go/v2/dataset"
	"github.com/ONSdigital/dp-api-clients-go/v2/health"
	"github.com/ONSdigital/dp-api-clients-go/v2/identity"
	"github.com/ONSdigital/dp-dimension-extractor/config"
	"github.com/ONSdigital/dp-dimension-extractor/event"
	"github.com/ONSdigital/dp-dimension-extractor/initialise"
	"github.com/ONSdigital/dp-dimension-extractor/service"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka/v2"
	dphttp "github.com/ONSdigital/dp-net/http"
	vault "github.com/ONSdigital/dp-vault"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
	"golang.org/x/net/context"
)

const authorizationHeader = "Authorization"

var (
	// BuildTime represents the time in which the service was built
	BuildTime string
	// GitCommit represents the commit (SHA-1) hash of the service that is running
	GitCommit string
	// Version represents the version of the service that is running
	Version string
)

func main() {
	log.Namespace = "dp-dimension-extractor"
	ctx := context.Background()
	log.Info(ctx, "starting dimension extractor")

	// Signals channel to notify only of SIGING and SIGTERM
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// Attempt to get config. Exit on failure.
	cfg, err := config.Get()
	exitIfError(ctx, "", err, nil)

	// Sensitive fields are omitted from config.String().
	log.Info(ctx, "config on startup", log.Data{"config": cfg})

	// Attempt to parse envMax from config. Exit on failure.
	envMax, err := strconv.ParseInt(cfg.KafkaMaxBytes, 10, 32)
	exitIfError(ctx, "encountered error parsing kafka max bytes", err, nil)

	// External services and their initialization state
	var serviceList initialise.ExternalServiceList

	// Get syncConsumerGroup Kafka Consumer
	syncConsumerGroup, err := serviceList.GetConsumer(ctx, cfg)
	exitIfError(ctx, "could not obtain consumer", err, nil)

	// Get AWS Session to access S3
	awsSession, s3Clients, err := serviceList.GetS3Clients(cfg)
	logIfError(ctx, "", err, nil)

	// Get dimensionExtracted Kafka Producer
	dimensionExtractedProducer, err := serviceList.GetProducer(
		ctx,
		cfg.DimensionsExtractedTopic,
		initialise.DimensionExtracted,
		int(envMax),
		cfg,
	)
	exitIfError(ctx, "", err, nil)

	// Get dimensionExtracted Error Kafka Producer
	dimensionExtractedErrProducer, err := serviceList.GetProducer(
		ctx,
		cfg.EventReporterTopic,
		initialise.DimensionExtractedErr,
		int(envMax),
		cfg,
	)
	exitIfError(ctx, "", err, nil)

	// If encryption is enabled, get Vault Client
	var vc *vault.Client
	if !cfg.EncryptionDisabled {
		vc, err = serviceList.GetVault(cfg, 3)
		logIfError(ctx, "", err, nil)
	}

	// Get Identity client for Zebedee serviceAuthToken validation
	zhc := health.NewClient("Zebedee", cfg.ZebedeeURL)
	idClient := identity.New(cfg.ZebedeeURL)

	// Dataset API Client with Max retries
	dc := dataset.NewAPIClientWithMaxRetries(cfg.DatasetAPIURL, cfg.MaxRetries)

	// Get HealthCheck and register checkers
	hc, err := serviceList.GetHealthCheck(cfg, BuildTime, GitCommit, Version)
	exitIfError(ctx, "", err, nil)
	if err := registerCheckers(ctx, &hc, !cfg.EncryptionDisabled, syncConsumerGroup, dimensionExtractedProducer, dimensionExtractedErrProducer, s3Clients, vc, zhc, dc); err != nil {
		os.Exit(1)
	}

	// create Channels
	eventLoopDone := make(chan bool)
	apiErrors := make(chan error, 1)

	// Create HTTP server for healthcheck
	router := mux.NewRouter()
	router.HandleFunc("/health", hc.Handler)
	hc.Start(ctx)
	httpServer := dphttp.NewServer(cfg.BindAddr, router)
	httpServer.HandleOSSignals = false // Disable this here to allow main to manage graceful shutdown of the entire app.

	go func() {
		log.Info(ctx, "starting api...")
		if err := httpServer.ListenAndServe(); err != nil {
			log.Error(ctx, "api http server returned error", err)
			hc.Stop()
			apiErrors <- err
		}
	}()

	service := &service.Service{
		AuthToken:                  cfg.ServiceAuthToken,
		DimensionExtractedProducer: dimensionExtractedProducer,
		EncryptionDisabled:         cfg.EncryptionDisabled,
		DatasetClient:              dc,
		AwsSession:                 awsSession,
		S3Clients:                  s3Clients,
		VaultClient:                vc,
		VaultPath:                  cfg.VaultPath,
	}

	// Get Error reporter
	errorReporter, err := serviceList.GetImportErrorReporter(dimensionExtractedErrProducer, log.Namespace)
	logIfError(ctx, "error while attempting to create error reporter client", err, nil)

	// Initialize event Consumer struct with initialized kafka consumers/producers and services
	eventConsumer := event.Consumer{
		KafkaConsumer: syncConsumerGroup,
		EventService:  service,
		ErrorReporter: errorReporter,
	}

	eventLoopContext, eventLoopCancel := context.WithCancel(ctx)

	// Validate this service against Zebedee
	serviceIdentityValidated := make(chan bool)
	go func() {
		if _, err = idClient.CheckTokenIdentity(eventLoopContext, cfg.ServiceAuthToken, identity.TokenTypeService); err != nil {
			log.Error(eventLoopContext, "could not obtain valid service account", err)
		} else {
			serviceIdentityValidated <- true
		}
	}()

	// Start Event Consumer (only if it is available)
	if serviceList.Consumer {
		eventConsumer.Start(eventLoopContext, eventLoopDone, serviceIdentityValidated)
	}

	// Log non-fatal errors, without exiting. Note that the structs and channels will always exist even if Kafka has not been initialised yet.
	syncConsumerGroup.Channels().LogErrors(ctx, "Kafka consumer error")
	dimensionExtractedProducer.Channels().LogErrors(ctx, "Kafka dimension extracted producer")
	dimensionExtractedErrProducer.Channels().LogErrors(ctx, "Kafka dimension extracted error producer")
	go func() {
		select {
		case apiError := <-apiErrors:
			log.Error(ctx, "server error", apiError)
		case <-eventLoopDone:
			log.Info(ctx, "event loop done")
		}
	}()

	// Block until a fatal error occurs
	select {
	case signal := <-signals:
		log.Info(ctx, "quitting after os signal received", log.Data{"signal": signal})
	}

	// give the app `Timeout` seconds to close gracefully before killing it.
	shutdownContext, cancel := context.WithTimeout(ctx, cfg.GracefulShutdownTimeout)

	go func() {

		// If kafka consumer exists, stop listening to it. (Will close later)
		if serviceList.Consumer {
			log.Info(shutdownContext, "stopping kafka consumer listener")
			syncConsumerGroup.StopListeningToConsumer(shutdownContext)
			log.Info(shutdownContext, "stopped kafka consumer listener")
		}

		eventLoopCancel()
		<-eventLoopDone

		// Shutdown HTTP server
		log.Info(shutdownContext, "closing http server")
		if err := httpServer.Shutdown(ctx); err != nil {
			log.Error(shutdownContext, "failed to gracefully close http server", err)
		}
		log.Info(ctx, "http server gracefully closed ")

		// Stop healthcheck
		hc.Stop()

		// If DimensionExtracted kafka producer exists, close it
		if serviceList.DimensionExtractedProducer {
			log.Info(shutdownContext, "closing kafka producer", log.Data{"producer": "DimensionExtracted"})
			dimensionExtractedProducer.Close(shutdownContext)
			log.Info(shutdownContext, "closed kafka producer", log.Data{"producer": "DimensionExtracted"})
		}

		// If DimensionExtractedError kafka producer exists, close it.
		if serviceList.DimensionExtractedErrProducer {
			log.Info(shutdownContext, "closing kafka producer", log.Data{"producer": "DimensionExtractedErr"})
			dimensionExtractedErrProducer.Close(shutdownContext)
			log.Info(shutdownContext, "closed kafka producer", log.Data{"producer": "DimensionExtractedErr"})
		}

		// If kafka consumer exists, close it.
		if serviceList.Consumer {
			log.Info(shutdownContext, "closing kafka consumer", log.Data{"consumer": "SyncConsumerGroup"})
			syncConsumerGroup.Close(shutdownContext)
			log.Info(shutdownContext, "closed kafka consumer", log.Data{"consumer": "SyncConsumerGroup"})
		}

		log.Info(shutdownContext, "done shutdown - cancelling timeout context")
		cancel() // stop timer
	}()

	// wait for timeout or success (via cancel)
	<-shutdownContext.Done()
	if shutdownContext.Err() == context.DeadlineExceeded {
		log.Error(shutdownContext, "shutdown timeout", shutdownContext.Err())
	} else {
		log.Error(shutdownContext, "done shutdown gracefully", errors.New("done shutdown gracefully"), log.Data{"context": shutdownContext.Err()})
	}
	os.Exit(1)
}

// registerCheckers adds the checkers for the provided clients to the healthcheck object.
// VaultClient health client will only be registered if encryption is enabled.
func registerCheckers(ctx context.Context, hc *healthcheck.HealthCheck, isEncryptionEnabled bool,
	kafkaConsumer *kafka.ConsumerGroup,
	dimensionExtractedProducer *kafka.Producer,
	dimensionExtractedErrProducer *kafka.Producer,
	s3Clients map[string]service.S3Client,
	vc *vault.Client,
	zebedeeHealthClient *health.Client,
	dc *dataset.Client) error {

	hasErrors := false

	if err := hc.AddCheck("Kafka Consumer", kafkaConsumer.Checker); err != nil {
		hasErrors = true
		log.Error(ctx, "error adding check for kafka consumer", err)
	}

	if err := hc.AddCheck("Kafka Producer", dimensionExtractedProducer.Checker); err != nil {
		hasErrors = true
		log.Error(ctx, "error adding check for kafka producer", err)
	}

	if err := hc.AddCheck("Kafka Error Producer", dimensionExtractedErrProducer.Checker); err != nil {
		hasErrors = true
		log.Error(ctx, "error adding check for kafka error producer", err)
	}

	for bucketName, s3 := range s3Clients {
		if err := hc.AddCheck(fmt.Sprintf("S3 bucket %s", bucketName), s3.Checker); err != nil {
			hasErrors = true
			log.Error(ctx, "error adding check for s3 client", err)
		}
	}

	if isEncryptionEnabled {
		if err := hc.AddCheck("Vault", vc.Checker); err != nil {
			hasErrors = true
			log.Error(ctx, "error adding check for vault", err)
		}
	}

	if err := hc.AddCheck("Zebedee", zebedeeHealthClient.Checker); err != nil {
		hasErrors = true
		log.Error(ctx, "error adding check for zebedee", err)
	}

	if err := hc.AddCheck("Dataset API", dc.Checker); err != nil {
		hasErrors = true
		log.Error(ctx, "error adding check for dataset api", err)
	}

	if hasErrors {
		return errors.New("Error(s) registering checkers for healthcheck")
	}
	return nil
}

// if error is not nil, log it and exit
func exitIfError(ctx context.Context, msg string, err error, data log.Data) {
	if err != nil {
		log.Error(ctx, fmt.Sprintf("fatal error %s", msg), err, data)
		os.Exit(1)
	}
}

// if error is not nil, log it only
func logIfError(ctx context.Context, msg string, err error, data log.Data) {
	if err != nil {
		log.Error(ctx, fmt.Sprintf("error %s", msg), err, data)
	}
}
