package main

import (
	"strconv"

	"github.com/ONSdigital/dp-dimension-extractor/config"
	"github.com/ONSdigital/dp-dimension-extractor/service"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
)

func main() {
	cfg := config.Get()

	log.Namespace = "dp-dimension-extractor"

	envMax, err := strconv.ParseInt(cfg.KafkaMaxBytes, 10, 32)
	if err != nil {
		log.ErrorC("encountered error parsing kafka max bytes", err, nil)
	}

	consumerGroup, err := kafka.NewConsumerGroup(cfg.Brokers, cfg.InputFileAvailableTopic, "input-file-available", cfg.InputFileAvailableOffset)
	if err != nil {
		log.ErrorC("could not obtain consumer", err, nil)
		panic("could not obtain consumer")
	}

	dimensionExtractedProducer := kafka.NewProducer(cfg.Brokers, cfg.DimensionsExtractedTopic, int(envMax))

	svc := &service.Service{
		EnvMax:        envMax,
		ConsumerGroup: consumerGroup,
		ImportAPIURL:  cfg.ImportAPIURL,
		Producer:      dimensionExtractedProducer,
	}

	svc.Start()
}
