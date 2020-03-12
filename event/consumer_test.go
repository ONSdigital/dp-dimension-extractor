package event

import (
	"errors"
	"testing"
	"time"

	"github.com/ONSdigital/dp-dimension-extractor/event/mocks"
	kafka "github.com/ONSdigital/dp-kafka"
	"github.com/ONSdigital/dp-kafka/kafkatest"
	"github.com/ONSdigital/dp-reporter-client/reporter/reportertest"
	"github.com/ONSdigital/log.go/log"
	. "github.com/smartystreets/goconvey/convey"
	"golang.org/x/net/context"
)

var ctx = context.Background()

func TestConsumer_Start(t *testing.T) {

	Convey("Given the Consumer has been configured correctly", t, func() {
		eventLoopDone := make(chan bool, 1)
		serviceIdentityValidated := make(chan bool, 1)

		msg := kafkatest.NewMessage(nil, 1)

		cgChannels := kafka.CreateConsumerGroupChannels(true)
		kafkaConsumerMock := kafkatest.NewMessageConsumerWithChannels(cgChannels, true)

		handler := &mocks.MessageHandler{
			EventLoopContextArgs: make([]context.Context, 0),
			MessageArgs:          make([]kafka.Message, 0),
			HandleMessageFunc: func(c context.Context, m kafka.Message) (string, error) {
				return "123456789", nil
			},
		}

		errorReporter := reportertest.NewImportErrorReporterMock(nil)

		consumer := &Consumer{
			KafkaConsumer: kafkaConsumerMock,
			EventService:  handler,
			ErrorReporter: errorReporter,
		}

		ctx, cancel := context.WithCancel(ctx)
		defer closeDown(t, cancel, eventLoopDone)

		Convey("When incoming receives a valid message", func() {
			consumer.Start(ctx, eventLoopDone, serviceIdentityValidated)
			serviceIdentityValidated <- true
			cgChannels.Upstream <- msg

			waitOrTimeout(t, eventLoopDone, cgChannels.UpstreamDone)

			Convey("Then handler.HandleMessage is called once with the expected parameters", func() {
				So(len(handler.MessageArgs), ShouldEqual, 1)
				So(len(handler.EventLoopContextArgs), ShouldEqual, 1)
			})

			Convey("And message.CommitAndRelease is called once", func() {
				So(len(kafkaConsumerMock.CommitAndReleaseCalls()), ShouldEqual, 1)
			})

			Convey("And errorReporter.Notify is never called", func() {
				So(len(errorReporter.NotifyCalls()), ShouldEqual, 0)
			})
		})
	})
}

func TestConsumer_HandleMessageError(t *testing.T) {
	Convey("Given a valid message", t, func() {

		eventLoopDone := make(chan bool, 1)
		serviceIdentityValidated := make(chan bool, 1)

		msg := kafkatest.NewMessage(nil, 1)

		cgChannels := kafka.CreateConsumerGroupChannels(true)
		kafkaConsumerMock := kafkatest.NewMessageConsumerWithChannels(cgChannels, true)

		errorReporter := reportertest.NewImportErrorReporterMock(nil)

		consumer := &Consumer{
			KafkaConsumer: kafkaConsumerMock,
			ErrorReporter: errorReporter,
		}

		ctx, cancel := context.WithCancel(ctx)
		defer closeDown(t, cancel, eventLoopDone)

		Convey("When handler.HandleMessage returns an error and an instanceID", func() {
			expectedErr := errors.New("bork")

			handler := &mocks.MessageHandler{
				EventLoopContextArgs: make([]context.Context, 0),
				MessageArgs:          make([]kafka.Message, 0),
				HandleMessageFunc: func(c context.Context, m kafka.Message) (string, error) {
					return "1234567890", expectedErr
				},
			}

			consumer.EventService = handler

			consumer.Start(ctx, eventLoopDone, serviceIdentityValidated)
			serviceIdentityValidated <- true
			cgChannels.Upstream <- msg

			waitOrTimeout(t, eventLoopDone, cgChannels.UpstreamDone)

			Convey("Then handler.HandleMessage is called once with the expected parameters", func() {
				So(len(handler.MessageArgs), ShouldEqual, 1)
				So(len(handler.EventLoopContextArgs), ShouldEqual, 1)
			})

			Convey("And message.CommitAndRelease is called once", func() {
				So(len(kafkaConsumerMock.CommitAndReleaseCalls()), ShouldEqual, 1)
			})

			Convey("And errorReporter.Notify is called once", func() {
				So(len(errorReporter.NotifyCalls()), ShouldEqual, 1)
			})
		})

		Convey("When handler.HandleMessage returns an error and an empty instanceID", func() {
			expectedErr := errors.New("bork")

			handler := &mocks.MessageHandler{
				EventLoopContextArgs: make([]context.Context, 0),
				MessageArgs:          make([]kafka.Message, 0),
				HandleMessageFunc: func(c context.Context, m kafka.Message) (string, error) {
					return "", expectedErr
				},
			}

			consumer.EventService = handler

			consumer.Start(ctx, eventLoopDone, serviceIdentityValidated)
			serviceIdentityValidated <- true
			cgChannels.Upstream <- msg

			waitOrTimeout(t, eventLoopDone, cgChannels.UpstreamDone)

			Convey("Then handler.HandleMessage is called once with the expected parameters", func() {
				So(len(handler.MessageArgs), ShouldEqual, 1)
				So(len(handler.EventLoopContextArgs), ShouldEqual, 1)
			})

			Convey("And message.CommitAndRelease is called once", func() {
				So(len(kafkaConsumerMock.CommitAndReleaseCalls()), ShouldEqual, 1)
			})

			Convey("And errorReporter.Notify is never called", func() {
				So(len(errorReporter.NotifyCalls()), ShouldEqual, 0)
			})
		})
	})
}

func waitOrTimeout(t *testing.T, eventLoopDone chan bool, expected chan bool) {
	select {
	case <-eventLoopDone:
		log.Event(nil, "Event loop done.", log.INFO)
	case <-expected:
		log.Event(nil, "expected behavior invoked", log.INFO)
	case <-time.After(time.Second * 3):
		log.Event(nil, "test timed out", log.INFO)
		t.FailNow()
	}
}

func closeDown(t *testing.T, cancel context.CancelFunc, eventLoopDone chan bool) {
	cancel()

	select {
	case <-eventLoopDone:
		log.Event(nil, "Close down successfully", log.INFO)
	case <-time.After(time.Second * 5):
		log.Event(nil, "consumer failed to stop.", log.INFO)
		t.FailNow()
	}
}
