package event

import (
	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/dp-reporter-client/reporter"
	"github.com/ONSdigital/log.go/v2/log"
	"golang.org/x/net/context"
)

// KafkaConsumer represents a Kafka consumer group instance
type KafkaConsumer interface {
	Channels() *kafka.ConsumerGroupChannels
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
				log.Info(eventLoopContext, "event loop context done", log.Data{"eventLoopContextErr": eventLoopContext.Err()})
				return
			case message := <-c.KafkaConsumer.Channels().Upstream:
				// In the future, kafka message will provice the context
				kafkaContext := context.Background()
				instanceID, err := c.EventService.HandleMessage(kafkaContext, message)
				if err != nil {
					log.Error(kafkaContext, "event failed to process", err, log.Data{"instance_id": instanceID})

					if len(instanceID) == 0 {
						log.Error(kafkaContext, "instance_id is empty, the error will not be reported", err)
					} else {
						err = c.ErrorReporter.Notify(instanceID, "event failed to process", err)
						if err != nil {
							log.Error(kafkaContext, "error while trying to report an error", err, log.Data{"instance_id": instanceID})
						}
					}

				} else {
					log.Info(kafkaContext, "event successfully processed", log.Data{"instance_id": instanceID})
				}
				message.CommitAndRelease()
				log.Info(eventLoopContext, "message committed and released", log.Data{"instance_id": instanceID})
			}
		}
	}()
}
