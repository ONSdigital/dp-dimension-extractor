package event

import (
	"github.com/ONSdigital/dp-reporter-client/reporter"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
	"golang.org/x/net/context"
)

// KafkaConsumer represents a Kafka consumer group instance
type KafkaConsumer interface {
	Incoming() chan kafka.Message
	CommitAndRelease(msg kafka.Message)
}

// Service is kafka message handler
type Service interface {
	HandleMessage(eventLoopContext context.Context, message kafka.Message) (string, error)
}

// Consumer polls a kafka topic for incoming messages
type Consumer struct {
	KafkaConsumer KafkaConsumer
	EventService  Service
	ErrorReporter reporter.ErrorReporter
}

// Start polling the kafka topic for incoming messages
func (c *Consumer) Start(eventLoopContext context.Context, eventLoopDone, serviceIdentityValidated chan bool) {
	go func() {
		defer close(eventLoopDone)
		// waiting to successfully validate service account (via zebedee)
		<-serviceIdentityValidated
		for {
			select {
			case <-eventLoopContext.Done():
				log.Trace("Event loop context done", log.Data{"eventLoopContextErr": eventLoopContext.Err()})
				return
			case message := <-c.KafkaConsumer.Incoming():

				instanceID, err := c.EventService.HandleMessage(eventLoopContext, message)
				if err != nil {
					log.ErrorC("event failed to process", err, log.Data{"instance_id": instanceID})

					if len(instanceID) == 0 {
						log.ErrorC("instance_id is empty errorReporter.Notify will not be called", err, nil)
					} else {
						err = c.ErrorReporter.Notify(instanceID, "event failed to process", err)
						if err != nil {
							log.ErrorC("errorReporter.Notify returned an error", err, log.Data{"instance_id": instanceID})
						}
					}

				} else {
					log.Debug("event successfully processed", log.Data{"instance_id": instanceID})
				}
				c.KafkaConsumer.CommitAndRelease(message)
				log.Debug("message committed", log.Data{"instance_id": instanceID})
			}
		}
	}()
}
