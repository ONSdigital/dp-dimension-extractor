package main

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net/http"
	"net/url"
	"os"
	"strconv"

	"fmt"
	"os/signal"
	"syscall"

	"github.com/ONSdigital/dp-dimension-extractor/api"
	"github.com/ONSdigital/dp-dimension-extractor/config"
	"github.com/ONSdigital/dp-dimension-extractor/event"
	"github.com/ONSdigital/dp-dimension-extractor/service"
	"github.com/ONSdigital/dp-reporter-client/reporter"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/rchttp"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"golang.org/x/net/context"
)

const authorizationHeader = "Authorization"

func main() {
	log.Namespace = "dp-dimension-extractor"

	cfg, err := config.Get()
	if err != nil {
		log.Error(err, nil)
		os.Exit(1)
	}

	// sensitive fields are omitted from config.String().
	log.Info("config on startup", log.Data{"config": cfg})

	envMax, err := strconv.ParseInt(cfg.KafkaMaxBytes, 10, 32)
	if err != nil {
		log.ErrorC("encountered error parsing kafka max bytes", err, nil)
		os.Exit(1)
	}

	syncConsumerGroup, err := kafka.NewSyncConsumer(cfg.Brokers, cfg.InputFileAvailableTopic, cfg.InputFileAvailableGroup, kafka.OffsetNewest)
	if err != nil {
		log.ErrorC("could not obtain consumer", err, nil)
		os.Exit(1)
	}

	var privateKey *rsa.PrivateKey
	if !cfg.EncryptionDisabled {
		privateKey, err = getPrivateKey([]byte(cfg.AWSPrivateKey))
		if err != nil {
			log.Error(err, nil)
			log.Info("you must provide a valid RSA private key for file upload encryption or set the environment variable ENCRYPTION_DISABLED to be true", nil)
			os.Exit(1)
		}
	}

	s3, err := session.NewSession(&aws.Config{Region: &cfg.AWSRegion})
	if err != nil {
		log.Error(err, nil)
		os.Exit(1)
	}

	dimensionExtractedProducer, err := kafka.NewProducer(cfg.Brokers, cfg.DimensionsExtractedTopic, int(envMax))
	if err != nil {
		log.Error(err, nil)
		os.Exit(1)
	}

	dimensionExtractedErrProducer, err := kafka.NewProducer(cfg.Brokers, cfg.EventReporterTopic, int(envMax))
	if err != nil {
		log.Error(err, nil)
		os.Exit(1)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	eventLoopDone := make(chan bool)
	apiErrors := make(chan error, 1)

	api.CreateDimensionExtractorAPI(cfg.DimensionExtractorURL, cfg.BindAddr, apiErrors)

	service := &service.Service{
		AuthToken:                  cfg.ServiceAuthToken,
		DatasetAPIURL:              cfg.DatasetAPIURL,
		DatasetAPIAuthToken:        cfg.DatasetAPIAuthToken,
		DimensionExtractedProducer: dimensionExtractedProducer,
		DimensionExtractorURL:      cfg.DimensionExtractorURL,
		EncryptionDisabled:         cfg.EncryptionDisabled,
		EnvMax:                     envMax,
		HTTPClient:                 rchttp.DefaultClient,
		MaxRetries:                 cfg.MaxRetries,
		PrivateKey:                 privateKey,
		S3:                         s3,
	}

	errorReporter, err := reporter.NewImportErrorReporter(dimensionExtractedErrProducer, log.Namespace)
	if err != nil {
		log.ErrorC("error while attempting to create error reporter client", err, nil)
		os.Exit(1)
	}

	eventConsumer := event.Consumer{
		KafkaConsumer: syncConsumerGroup,
		EventService:  service,
		ErrorReporter: errorReporter,
	}

	eventLoopContext, eventLoopCancel := context.WithCancel(context.Background())

	serviceIdentityValidated := make(chan bool)
	go func() {
		if err = checkServiceIdentity(eventLoopContext, cfg.ZebedeeURL, cfg.ServiceAuthToken); err != nil {
			log.ErrorC("could not obtain valid service account", err, nil)
			eventLoopDone <- true
		}
		serviceIdentityValidated <- true
	}()

	eventConsumer.Start(eventLoopContext, eventLoopDone, serviceIdentityValidated)

	// block until a fatal error, signal or eventLoopDone - then proceed to shutdown
	select {
	case <-eventLoopDone:
		log.Debug("quitting after done was closed", nil)
	case signal := <-signals:
		log.Debug("quitting after os signal received", log.Data{"signal": signal})
	case consumerError := <-syncConsumerGroup.Errors():
		log.Error(fmt.Errorf("aborting consumer"), log.Data{"message_received": consumerError})
	case producerError := <-service.DimensionExtractedProducer.Errors():
		log.Error(fmt.Errorf("aborting producer"), log.Data{"message_received": producerError})
	case <-apiErrors:
		log.Error(fmt.Errorf("server error forcing shutdown"), nil)
	}

	// give the app `Timeout` seconds to close gracefully before killing it.
	ctx, cancel := context.WithTimeout(context.Background(), cfg.GracefulShutdownTimeout)

	go func() {
		log.Debug("stopping kafka consumer listener", nil)
		syncConsumerGroup.StopListeningToConsumer(ctx)
		log.Debug("stopped kafka consumer listener", nil)
		eventLoopCancel()
		<-eventLoopDone
		log.Debug("closing http server", nil)
		if err := api.Close(ctx); err != nil {
			log.ErrorC("failed to gracefully close http server", err, nil)
		} else {
			log.Debug("gracefully closed http server", nil)
		}
		log.Debug("closing dimension extracted kafka producer", nil)
		service.DimensionExtractedProducer.Close(ctx)
		log.Debug("closed dimension extracted kafka producer", nil)

		log.Debug("closing down dimension extracted error producer", nil)
		dimensionExtractedErrProducer.Close(ctx)
		log.Debug("closed dimension extracted error producer", nil)

		log.Debug("closing kafka consumer", nil)
		syncConsumerGroup.Close(ctx)
		log.Debug("closed kafka consumer", nil)

		log.Info("done shutdown - cancelling timeout context", nil)

		cancel() // stop timer
	}()

	// wait for timeout or success (via cancel)
	<-ctx.Done()
	if ctx.Err() == context.DeadlineExceeded {
		log.Error(ctx.Err(), nil)
	} else {
		log.Info("done shutdown gracefully", log.Data{"context": ctx.Err()})
	}
	os.Exit(1)
}

func getPrivateKey(keyBytes []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(keyBytes)
	if block == nil || block.Type != "RSA PRIVATE KEY" {
		return nil, errors.New("invalid RSA PRIVATE KEY provided")
	}

	return x509.ParsePKCS1PrivateKey(block.Bytes)
}

func checkServiceIdentity(ctx context.Context, zebedeeURL, serviceAuthToken string) error {
	// TODO switch out below to use gedges go-ns package
	client := rchttp.DefaultClient

	path := fmt.Sprintf("%s/identity", zebedeeURL)

	var URL *url.URL
	URL, err := url.Parse(path)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("GET", URL.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set(authorizationHeader, serviceAuthToken)

	res, err := client.Do(ctx, req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid status [%d] returned from [%s]", res.StatusCode, zebedeeURL)
	}

	log.Info("dimension extractor has a valid service account", nil)
	return nil
}
