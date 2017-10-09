package main

import (
	"os"
	"strconv"

	"github.com/ONSdigital/dp-dimension-extractor/config"
	"github.com/ONSdigital/dp-dimension-extractor/service"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/s3"
)

func main() {
	cfg, err := config.Get()
	if err != nil {
		log.Error(err, nil)
		os.Exit(1)
	}

	log.Namespace = "dp-dimension-extractor"

	envMax, err := strconv.ParseInt(cfg.KafkaMaxBytes, 10, 32)
	if err != nil {
		log.ErrorC("encountered error parsing kafka max bytes", err, nil)
		os.Exit(1)
	}

	consumerGroup, err := kafka.NewConsumerGroup(cfg.Brokers, cfg.InputFileAvailableTopic, cfg.InputFileAvailableGroup, kafka.OffsetNewest)
	if err != nil {
		log.ErrorC("could not obtain consumer", err, nil)
		os.Exit(1)
	}

	s3, err := s3.New(cfg.AWSRegion)
	if err != nil {
		log.Error(err, nil)
		os.Exit(1)
	}

	dimensionExtractedProducer, err := kafka.NewProducer(cfg.Brokers, cfg.DimensionsExtractedTopic, int(envMax))
	if err != nil {
		log.Error(err, nil)
		os.Exit(1)
	}

	svc := &service.Service{
		EnvMax:                envMax,
		BindAddr:              cfg.BindAddr,
		Consumer:              consumerGroup,
		DatasetAPIURL:         cfg.DatasetAPIURL,
		DatasetAPIAuthToken:   cfg.DatasetAPIAuthToken,
		DimensionExtractorURL: cfg.DimensionExtractorURL,
		MaxRetries:            cfg.MaxRetries,
		Producer:              dimensionExtractedProducer,
		S3:                    s3,
		Shutdown:              cfg.ShutdownTimeout,
	}

	svc.Start()
}
