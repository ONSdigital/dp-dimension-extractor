package mocks

import (
	"github.com/ONSdigital/go-ns/kafka"
	"golang.org/x/net/context"
)

// MessageHandler provides mocked functionality for a event.MessageHandler
type MessageHandler struct {
	EventLoopContextArgs []context.Context
	MessageArgs          []kafka.Message
	HandleMessageFunc    func(c context.Context, m kafka.Message) (string, error)
}

// HandleMessage captures method parameters and returns the configured values
func (m *MessageHandler) HandleMessage(c context.Context, msg kafka.Message) (string, error) {
	m.EventLoopContextArgs = append(m.EventLoopContextArgs, c)
	m.MessageArgs = append(m.MessageArgs, msg)
	return m.HandleMessageFunc(c, msg)
}

// KafkaConsumer is a mocked implementation of a kafka consumer
type KafkaConsumer struct {
	IncomingCalls          int
	IncomingArg            chan kafka.Message
	CommitAndReleaseCalls  int
	CommitAndReleaseNotify chan bool
	ReleaseCalls           int
	ReleaseNotify          chan bool
}

// Incoming mocked impl of Incoming
func (m *KafkaConsumer) Incoming() chan kafka.Message {
	m.IncomingCalls++
	return m.IncomingArg
}

// CommitAndRelease mocked impl of CommitAndRelease
func (m *KafkaConsumer) CommitAndRelease(msg kafka.Message) {
	m.CommitAndReleaseCalls++
	close(m.CommitAndReleaseNotify)
}

// Release mocked impl of Release
func (m *KafkaConsumer) Release() {
	m.ReleaseCalls++
	close(m.ReleaseNotify)
}

// KafkaMessage represents a single kafka message.
type KafkaMessage struct {
	Data      []byte
	OffsetVal int64
}

// GetData mocked impl of GetData
func (k KafkaMessage) GetData() []byte {
	return k.Data
}

// Commit mocked impl of Commit
func (k KafkaMessage) Commit() {
}

// Offset mocked impl of Offset
func (k KafkaMessage) Offset() int64 {
	return k.OffsetVal
}
