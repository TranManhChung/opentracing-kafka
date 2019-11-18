package config

import (
	"log"
	"sync"

	"github.com/spf13/viper"
)

// AppConfig is structure holding all application configurations
type AppConfig interface {
	GetKafkaBrokers() []string
	GetKafkaTopics() []string
}

type appConfig struct {
	kafkaBrokers []string
	kafkaTopics  []string
}

func (c appConfig) GetKafkaBrokers() []string {
	return c.kafkaBrokers
}

func (c appConfig) GetKafkaTopics() []string {
	return c.kafkaTopics
}

var config appConfig
var once sync.Once

// LoadConfig method to load application configurations from config/app.toml file.
func LoadConfig() AppConfig {
	once.Do(func() {
		viper.SetConfigName("app")
		viper.AddConfigPath("config")

		err := viper.ReadInConfig()
		if err != nil {
			log.Fatalf("Read configs failed: %v\n", err)
			panic(err)
		}

		brokers := viper.GetStringSlice("kafka.brokers")
		topics := viper.GetStringSlice("kafka.topics")

		config = appConfig{
			kafkaBrokers: brokers,
			kafkaTopics:  topics,
		}
	})
	return config
}
