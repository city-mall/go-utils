package consumer

import (
	"time"

	zerolog "github.com/rs/zerolog/log"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

var (
	consumer    *kafka.Consumer
	initialized = false
	err         error
	run         = true
)

type ConsumerConfig struct {
	AppEnv        string
	ClientID      string
	SASLEnable    bool
	SASLMechanism string
	SASLUser      string
	SASLPassword  string
	Brokers       string
	ConsumerGroup string
	Topic         []string
}

func KafkaConsumer(config ConsumerConfig) {
	appEnv := config.AppEnv
	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers": config.Brokers,
		"client.id":         config.ClientID,
		"group.id":          config.ConsumerGroup,
		"auto.offset.reset": "earliest",
	}

	if appEnv != "development" {
		kafkaConfig.SetKey("sasl.mechanisms", config.SASLMechanism)
		kafkaConfig.SetKey("sasl.username", config.SASLUser)
		kafkaConfig.SetKey("sasl.password", config.SASLPassword)
		kafkaConfig.SetKey("security.protocol", "sasl_ssl")
	}

	consumer, err = kafka.NewConsumer(kafkaConfig)

	err = consumer.SubscribeTopics(*&config.Topic, nil)
	if err != nil {
		zerolog.Fatal().Msgf(err.Error())
	}

	initialized = true
	zerolog.Info().Msgf("Consumer connected")
}

func Consume(callback func(*kafka.Message)) {
	if !initialized {
		zerolog.Error().Msgf("Consumer not initialized")
		return
	}
	for run == true {
		select {
		default:
			msg, err := consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				// Errors are informational and automatically handled by the consumer
				continue
			}
			callback(msg)
		}
	}
	consumer.Close()
}

func CloseConsumer() {
	if !initialized {
		zerolog.Error().Msgf("Consumer not initialized")
		return
	}
	run = false
}
