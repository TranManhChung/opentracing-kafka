package consumer

import (
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"gitlab.360live.vn/zpi/common/zpi-common/log"
	"go.uber.org/zap"
)

// KafkaConsumers wrapper for kafka consumer
type KafkaConsumers struct {
	Brokers  []string
	GroupID  string
	Topics   []string
	logger   log.Factory
	consumer *cluster.Consumer
	Process  func(message *sarama.ConsumerMessage)
}

//NewKafkaConsumers ...
func NewKafkaConsumers(brokers []string, groupID string, topics []string, process func(message *sarama.ConsumerMessage), logger log.Factory) (*KafkaConsumers, error) {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Group.Mode = cluster.ConsumerModePartitions
	//-------------------------------
	config.Version = sarama.V1_0_0_0
	//-------------------------------

	consumer, err := cluster.NewConsumer(brokers, groupID, topics, config)
	if err != nil {
		logger.Bg().Fatal("[newKafkaConsumers] fail to new consumer", zap.Error(err))
	}
	return &KafkaConsumers{
		Brokers:  brokers,
		GroupID:  groupID,
		Topics:   topics,
		logger:   logger,
		consumer: consumer,
		Process:  process,
	}, nil
}

// Start begin subscribing
func (c *KafkaConsumers) Start() {
	defer c.consumer.Close()

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// handle errors encountered while consuming events.
	go c.handleConsumeErrors()

	// handle rebalanced notification events.
	go c.handleConsumeNotifications()

	for {
		select {
		case part, ok := <-c.consumer.Partitions():
			if !ok {
				return
			}
			// start a separate goroutine to consume messages
			go func(pc cluster.PartitionConsumer) {
				for msg := range pc.Messages() {
					c.Process(msg)
					c.consumer.MarkOffset(msg, "") // mark message as processed
				}
			}(part)

		case <-signals:
			return
		}
	}
}

func (c *KafkaConsumers) handleConsumeErrors() {
	for err := range c.consumer.Errors() {
		c.logger.Bg().Error("[handleConsumeErrors]", zap.Error(err))
	}
}

func (c *KafkaConsumers) handleConsumeNotifications() {
	for notification := range c.consumer.Notifications() {
		c.logger.Bg().Info("[handleConsumeNotifications] Rebalanced", zap.Reflect("notification", notification))
	}
}
