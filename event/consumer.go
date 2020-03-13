package event

import (
	kafka "github.com/ONSdigital/dp-kafka"
	"github.com/ONSdigital/dp-reporter-client/reporter"
	"github.com/ONSdigital/log.go/log"
	"golang.org/x/net/context"
)

// KafkaConsumer represents a Kafka consumer group instance
type KafkaConsumer interface {
	Channels() *kafka.ConsumerGroupChannels
	CommitAndRelease(msg kafka.Message)
}

// Service is kafka message handler
type Service interface {
	HandleMessage(ctx context.Context, message kafka.Message) (string, error)
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
				log.Event(eventLoopContext, "Event loop context done", log.INFO, log.Data{"eventLoopContextErr": eventLoopContext.Err()})
				return
			case message := <-c.KafkaConsumer.Channels().Upstream:
				// In the future, kafka message will provice the context
				kafkaContext := context.Background()
				instanceID, err := c.EventService.HandleMessage(kafkaContext, message)
				if err != nil {
					log.Event(kafkaContext, "event failed to process", log.ERROR, log.Error(err), log.Data{"instance_id": instanceID})

					if len(instanceID) == 0 {
						log.Event(kafkaContext, "instance_id is empty errorReporter.Notify will not be called", log.ERROR, log.Error(err))
					} else {
						err = c.ErrorReporter.Notify(instanceID, "event failed to process", err)
						if err != nil {
							log.Event(kafkaContext, "errorReporter.Notify returned an error", log.ERROR, log.Error(err), log.Data{"instance_id": instanceID})
						}
					}

				} else {
					log.Event(kafkaContext, "event successfully processed", log.INFO, log.Data{"instance_id": instanceID})
				}
				c.KafkaConsumer.CommitAndRelease(message)
				log.Event(eventLoopContext, "message committed", log.INFO, log.Data{"instance_id": instanceID})
			}
		}
	}()
}
