package errorhandler

import (
	errorhandler "github.com/ONSdigital/dp-import-reporter/handler"
	eventSchema "github.com/ONSdigital/dp-import-reporter/schema"

	"github.com/ONSdigital/go-ns/log"
)

//go:generate moq -out ../mocks/handler.go -pkg errorstest . Handler MessageProducer

var _ Handler = (*KafkaHandler)(nil)

//Handler defines the interface for handling errors
type Handler interface {
	Handle(instanceId string, err error, data log.Data)
}

//KafkaHandler provides an error hadler that writes to a kafka error topic
type KafkaHandler struct {
	messageProducer MessageProducer
}

//NewKafkaHandler returns a new kafkahandler that sends error messages to the given messageProducer
func NewKafkaHandler(messageProducer MessageProducer) *KafkaHandler {
	return &KafkaHandler{
		messageProducer: messageProducer,
	}
}

//MessageProducer dependency that writes the messages
type MessageProducer interface {
	Output() chan []byte
	Closer() chan bool
}

//Handle logs the errors and sends it to the error reporter kafka
func (handler *KafkaHandler) Handle(instanceID string, err error, data log.Data) {

	if data == nil {
		data = log.Data{"INSTANCEID": instanceID, "ERROR": err.Error()}
	}
	log.Info("Recieved error report", data)

	eventReport := errorhandler.EventReport{
		InstanceID: instanceID,
		EventType:  "error",
		EventMsg:   err.Error(),
	}
	errMsg, err := eventSchema.ReportedEventSchema.Marshal(eventReport)
	if err != nil {
		log.ErrorC("Failed to marshall error to event-reporter", err, data)
		return
	}

	handler.messageProducer.Output() <- errMsg
}
