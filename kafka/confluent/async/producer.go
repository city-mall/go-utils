package async

import (
	"context"
	"sync"
	"time"

	zerolog "github.com/rs/zerolog/log"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type producer struct {
	asyncProducer *kafka.Producer
	config        ProducerConfig
	initialized   bool
	err           error
}

var producers = make(map[string]*producer)

type ProducerConfig struct {
	AppEnv                             string
	ProducerTimeout                    time.Duration
	ClientID                           string
	ProducerGroup                      string
	SASLMechanism                      string
	SASLUser                           string
	SASLPassword                       string
	Brokers                            string
	EnableSslCertificationVerification bool
	Name                               string
	Batch                              int
	BatchTimeout                       int
}

func KafkaProducer(config ProducerConfig) {
	kafkaConfig := &kafka.ConfigMap{
		"client.id":          config.ClientID,
		"group.id":           config.ProducerGroup,
		"bootstrap.servers":  config.Brokers,
		"socket.timeout.ms":  config.ProducerTimeout,
		"batch.num.messages": config.Batch,
		"linger.ms":          config.BatchTimeout,
	}
	appEnv := config.AppEnv
	if appEnv != "development" {
		kafkaConfig.SetKey("sasl.mechanisms", config.SASLMechanism)
		kafkaConfig.SetKey("security.protocol", "SASL_SSL")
		kafkaConfig.SetKey("sasl.username", config.SASLUser)
		kafkaConfig.SetKey("sasl.password", config.SASLPassword)
		kafkaConfig.SetKey("enable.ssl.certificate.verification", config.EnableSslCertificationVerification)
	}
	producers[config.Name] = &producer{
		asyncProducer: nil,
		initialized:   false,
		err:           nil,
		config:        config,
	}
	producers[config.Name].asyncProducer, producers[config.Name].err = kafka.NewProducer(kafkaConfig)
	if producers[config.Name].err != nil {
		zerolog.Error().Msgf(producers[config.Name].err.Error())
	} else {
		producers[config.Name].initialized = true
		zerolog.Info().Msgf("Kafka connected %v", config.Name)
	}
}

func CreateTopic(topics []string, env string, name string) error {
	if producer, found := producers[name]; found {
		zerolog.Info().Msgf("CreateTopic: topic=%s", topics)
		a, err := kafka.NewAdminClientFromProducer(producer.asyncProducer)
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
	} else {
		zerolog.Error().Msgf("Producer %v not found", name)
		return nil
	}
}

func PushJSONMessage(m []byte, topic string, name string) {
	if producer, found := producers[name]; found {
		if !producer.initialized {
			zerolog.Error().Msgf("Kafka %v not initialized", name)
			return
		}
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          m,
		}
		e := producer.asyncProducer.Produce(msg, nil)
		if e != nil {
			zerolog.Error().Msgf(e.Error())
		}
	} else {
		zerolog.Error().Msgf("Producer %v not found", name)
	}
}

func PushStringMessage(m string, topic string, name string) {
	if producer, found := producers[name]; found {
		if !producer.initialized {
			zerolog.Error().Msgf("Kafka %v not initialized", name)
			return
		}
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(m),
		}
		e := producer.asyncProducer.Produce(msg, nil)
		if e != nil {
			zerolog.Error().Msgf(e.Error())
		}
	} else {
		zerolog.Error().Msgf("Producer %v not found", name)
	}
}

func CloseProducer(name string, wg *sync.WaitGroup) {
	if producer, found := producers[name]; found {
		if !producer.initialized {
			zerolog.Error().Msgf("Kafka %v not initialized", name)
			return
		}
		producer.asyncProducer.Flush(producer.config.BatchTimeout)
		zerolog.Info().Msgf("Closing %v", name)
		producer.asyncProducer.Close()
	} else {
		zerolog.Error().Msgf("Producer %v not found", name)
	}
	if wg != nil {
		wg.Done()
	}
}
