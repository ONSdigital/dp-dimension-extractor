package mocks

import (
	kafka "github.com/ONSdigital/dp-kafka"
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
