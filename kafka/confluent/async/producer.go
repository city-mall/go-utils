package async

import (
	"time"

	zerolog "github.com/rs/zerolog/log"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

var (
	asyncProducer *kafka.Producer
	initialized   = false
	err           error
)

type ProducerConfig struct {
	AppEnv          string
	ProducerTimeout time.Duration
	ClientID        string
	SASLMechanism   string
	SASLUser        string
	SASLPassword    string
	Brokers         string
}

func KafkaProducer(config ProducerConfig) {
	kafkaConfig := &kafka.ConfigMap{
		"client.id":         config.ClientID,
		"bootstrap.servers": config.Brokers,
		"socket.timeout.ms": config.ProducerTimeout,
	}
	appEnv := config.AppEnv
	if appEnv != "development" {
		kafkaConfig.SetKey("sasl.mechanisms", config.SASLMechanism)
		kafkaConfig.SetKey("security.protocol", "SASL_SSL")
		kafkaConfig.SetKey("sasl.username", config.SASLUser)
		kafkaConfig.SetKey("sasl.password", config.SASLPassword)
	}
	asyncProducer, err = kafka.NewProducer(kafkaConfig)
	if err != nil {
		zerolog.Error().Msgf(err.Error())
	} else {
		initialized = true
		zerolog.Info().Msgf("Kafka connected")
	}
}

func PushJSONMessage(m []byte, topic string) {
	if !initialized {
		zerolog.Error().Msgf("Kafka not initialized")
		return
	}
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          m,
	}
	e := asyncProducer.Produce(msg, nil)
	if e != nil {
		zerolog.Error().Msgf(e.Error())
	}
}

func PushStringMessage(m string, topic string) {
	if !initialized {
		zerolog.Error().Msgf("Kafka not initialized")
		return
	}
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(m),
	}
	e := asyncProducer.Produce(msg, nil)
	if e != nil {
		zerolog.Error().Msgf(e.Error())
	}
}

func CloseProducer() {
	if !initialized {
		zerolog.Error().Msgf("Kafka not initialized")
		return
	}
	asyncProducer.Close()
}
