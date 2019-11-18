package producer

import (
	"encoding/json"

	"github.com/Shopify/sarama"
)

// Message ...
type Message struct {
	Topic string
	Key   string
	Data  interface{}

	Headers []sarama.RecordHeader
}

// MessageDispatcher ...
type MessageDispatcher struct {
	producer       sarama.SyncProducer
	done           chan string
	MessageChannel chan Message
}

// NewDispatcher method to create SyncProducer
func NewDispatcher(brokers []string, partitioner sarama.PartitionerConstructor) (*MessageDispatcher, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = partitioner
	config.Producer.Retry.Max = 3
	config.Producer.Return.Successes = true
	//-------------------------------
	config.Version = sarama.V1_0_0_0
	//-------------------------------

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}
	return &MessageDispatcher{
		producer:       producer,
		done:           make(chan string),
		MessageChannel: make(chan Message, 5000),
	}, nil
}

// Start ...
func (d *MessageDispatcher) Start() {
	go func() {
		for {
			select {
			case data := <-d.MessageChannel:
				d.sendMessage(data)
			case <-d.done:
				d.producer.Close()
				return
			}
		}
	}()
}

// Close ...
func (d *MessageDispatcher) Close() {
	close(d.done)
}

func (d *MessageDispatcher) sendMessage(msg Message) error {
	json, err := json.Marshal(msg.Data)
	if err != nil {
		return err
	}

	producerMessage := &sarama.ProducerMessage{
		Topic:   msg.Topic,
		Value:   sarama.StringEncoder(string(json)),
		Headers: msg.Headers,
	}

	if msg.Key != "" {
		producerMessage.Key = sarama.StringEncoder(msg.Key)
	}

	_, _, err = d.producer.SendMessage(producerMessage)
	return err
}
