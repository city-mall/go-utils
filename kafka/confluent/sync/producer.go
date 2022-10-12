package async

import (
	"context"
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
		zerolog.Error().Msgf(err.Error())
	} else {
		initialized = true
		zerolog.Info().Msgf("Kafka connected")
	}
}

func CreateTopic(topics []string, env string) error {

	zerolog.Info().Msgf("CreateTopic: topic=%s", topics)
	a, err := kafka.NewAdminClientFromProducer(syncProducer)
	if err != nil {
		return err
	}
	// Contexts are used to abort or limit the amount of time
	// the Admin call blocks waiting for a result.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Create topics on cluster.
	// Set Admin options to wait up to 60s for the operation to finish on the remote cluster
	maxDur, err := time.ParseDuration("60s")
	if err != nil {
		return err
	}
	NumPartitions := 10
	ReplicationFactor := 1
	if env != "production" {
		NumPartitions = 1
		ReplicationFactor = 1
	}

	var topicSpecs []kafka.TopicSpecification

	for _, topic := range topics {
		topicSpecs = append(topicSpecs, kafka.TopicSpecification{
			Topic:             topic,
			NumPartitions:     NumPartitions,
			ReplicationFactor: ReplicationFactor})
	}

	results, err := a.CreateTopics(
		ctx,
		// Multiple topics can be created simultaneously
		// by providing more TopicSpecification structs here.
		topicSpecs,
		// Admin options
		kafka.SetAdminOperationTimeout(maxDur), kafka.SetAdminRequestTimeout(maxDur))
	if err != nil {
		zerolog.Error().Err(err).Msgf("error creating topic %v", topics)
		return err
	}
	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError && result.Error.Code() != kafka.ErrTopicAlreadyExists {
			return err
		}
		zerolog.Info().Msgf("%v\n", result)
	}
	a.Close()
	return err
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
		zerolog.Error().Msgf(err.Error())
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
		zerolog.Error().Msgf(err.Error())
	}
}

func CloseProducer() {
	if !initialized {
		zerolog.Error().Msgf("Kafka not initialized")
		return
	}
	syncProducer.Close()
}
