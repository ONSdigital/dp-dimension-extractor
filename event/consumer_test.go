package event

import (
	"errors"
	"testing"
	"time"

	"github.com/ONSdigital/dp-dimension-extractor/event/mocks"
	"github.com/ONSdigital/dp-reporter-client/reporter/reportertest"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
	. "github.com/smartystreets/goconvey/convey"
	"golang.org/x/net/context"
)

func TestConsumer_Start(t *testing.T) {

	Convey("Given the Consumer has been configured correctly", t, func() {
		eventLoopDone := make(chan bool, 1)
		incomingChan := make(chan kafka.Message, 1)
		commitAndRelease := make(chan bool, 1)

		msg := mocks.KafkaMessage{
			OffsetVal: 1,
			Data:      nil,
		}

		kafkaConsumerMock := &mocks.KafkaConsumer{
			CommitAndReleaseCalls:  0,
			CommitAndReleaseNotify: commitAndRelease,
			IncomingCalls:          0,
			IncomingArg:            incomingChan,
		}

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

		ctx, cancel := context.WithCancel(context.Background())
		defer closeDown(t, cancel, eventLoopDone)

		Convey("When incoming receives a valid message", func() {
			consumer.Start(ctx, eventLoopDone)
			incomingChan <- msg

			waitOrTimeout(t, eventLoopDone, commitAndRelease)

			Convey("Then handler.HandleMessage is called once with the expected parameters", func() {
				So(len(handler.MessageArgs), ShouldEqual, 1)
				So(len(handler.EventLoopContextArgs), ShouldEqual, 1)
			})

			Convey("And message.CommitAndRelease is called once", func() {
				So(kafkaConsumerMock.CommitAndReleaseCalls, ShouldEqual, 1)
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
		incomingChan := make(chan kafka.Message, 1)
		release := make(chan bool, 1)

		msg := mocks.KafkaMessage{
			OffsetVal: 1,
			Data:      nil,
		}

		kafkaConsumerMock := &mocks.KafkaConsumer{
			CommitAndReleaseCalls:  0,
			CommitAndReleaseNotify: release,
			IncomingCalls:          0,
			IncomingArg:            incomingChan,
		}

		errorReporter := reportertest.NewImportErrorReporterMock(nil)

		consumer := &Consumer{
			KafkaConsumer: kafkaConsumerMock,
			ErrorReporter: errorReporter,
		}

		ctx, cancel := context.WithCancel(context.Background())
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

			consumer.Start(ctx, eventLoopDone)

			incomingChan <- msg

			waitOrTimeout(t, eventLoopDone, release)

			Convey("Then handler.HandleMessage is called once with the expected parameters", func() {
				So(len(handler.MessageArgs), ShouldEqual, 1)
				So(len(handler.EventLoopContextArgs), ShouldEqual, 1)
			})

			Convey("And message.CommitAndRelease is called once", func() {
				So(kafkaConsumerMock.CommitAndReleaseCalls, ShouldEqual, 1)
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

			consumer.Start(ctx, eventLoopDone)

			incomingChan <- msg

			waitOrTimeout(t, eventLoopDone, release)

			Convey("Then handler.HandleMessage is called once with the expected parameters", func() {
				So(len(handler.MessageArgs), ShouldEqual, 1)
				So(len(handler.EventLoopContextArgs), ShouldEqual, 1)
			})

			Convey("And message.CommitAndRelease is called once", func() {
				So(kafkaConsumerMock.CommitAndReleaseCalls, ShouldEqual, 1)
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
		log.Debug("Event loop done.", nil)
	case <-expected:
		log.Debug("expected behavior invoked", nil)
	case <-time.After(time.Second * 3):
		log.Debug("test timed out", nil)
		t.FailNow()
	}
}

func closeDown(t *testing.T, cancel context.CancelFunc, eventLoopDone chan bool) {
	cancel()

	select {
	case <-eventLoopDone:
		log.Debug("Close down successfully", nil)
	case <-time.After(time.Second * 5):
		log.Debug("consumer failed to stop.", nil)
		t.FailNow()
	}
}
