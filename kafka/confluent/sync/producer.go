package async

import (
	"time"

	zerolog "github.com/rs/zerolog/log"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

var (
	syncProducer *kafka.Producer
	initialized  = false
	err          error
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
		kafkaConfig.SetKey("security.protocol", "sasl_ssl")
		kafkaConfig.SetKey("sasl.username", config.SASLUser)
		kafkaConfig.SetKey("sasl.password", config.SASLPassword)
	}
	syncProducer, err = kafka.NewProducer(kafkaConfig)
	if err != nil {
		zerolog.Fatal().Msgf(err.Error())
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
	delivery_chan := make(chan kafka.Event, 10000)
	defer close(delivery_chan)
	syncProducer.Produce(msg, delivery_chan)
	e := <-delivery_chan
	ack := e.(*kafka.Message)

	if ack.TopicPartition.Error != nil {
		zerolog.Fatal().Msgf(err.Error())
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
	delivery_chan := make(chan kafka.Event, 10000)
	defer close(delivery_chan)
	syncProducer.Produce(msg, delivery_chan)
	e := <-delivery_chan
	ack := e.(*kafka.Message)

	if ack.TopicPartition.Error != nil {
		zerolog.Fatal().Msgf(err.Error())
	}
}

func CloseProducer() {
	if !initialized {
		zerolog.Error().Msgf("Kafka not initialized")
		return
	}
	syncProducer.Close()
}
