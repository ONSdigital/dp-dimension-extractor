package errorhandler_test

import (
	"errors"
	"testing"

	"github.com/ONSdigital/dp-dimension-extractor/errors"
	"github.com/ONSdigital/dp-dimension-extractor/mocks"
	"github.com/ONSdigital/go-ns/kafka/kafkatest"
	"github.com/ONSdigital/go-ns/log"
	. "github.com/smartystreets/goconvey/convey"
)

type file struct {
	instance string
}

func TestSpec(t *testing.T) {

	Convey("Given an error handler", t, func() {

		Convey("When error handler is called ", func() {
			errHandle := &mocks.HandlerMock{
				HandleFunc: func(instanceId string, err error, data log.Data) {
					//
				},
			}
			errHandle.Handle("a4695fee-f0a2-49c4-b136-e3ca8dd40476", errors.New("error"), nil)
			Convey("And a complete run through should have 1 call to the handle", func() {
				So(len(errHandle.HandleCalls()), ShouldEqual, 1)
			})
		})
	})
}

func TestKafkaProducer(t *testing.T) {
	Convey("Given a new error kafka producer is created", t, func() {
		Convey("When a new kafka producer is created", func() {
			outputChannel := make(chan []byte, 1)
			mockMessageProducer := kafkatest.NewMessageProducer(outputChannel, nil, nil)

			errorHandler := errorhandler.NewKafkaHandler(mockMessageProducer)
			Convey("And the error kafka is not nil", func() {
				So(errorHandler, ShouldNotBeNil)
			})
			errorHandler.Handle("instanceId", errors.New("1"), nil)

		})
	})
}
