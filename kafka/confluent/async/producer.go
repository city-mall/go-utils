package async

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	zerolog "github.com/rs/zerolog/log"
)

/*

----------------------------Batching controlled/enabled by these three parameters -------------------------------------

batch.size+>  is a configuration property in the Kafka producer that controls the maximum size of a batch of messages that
                   are sent to the server in a single request.

 BatchNumMessages => batch.num.messages is a configuration property in the Kafka producer that controls the maximum number of messages
                     that are included in a single batch.

 LingerMs => linger.ms is a configuration property in the Kafka producer that controls the amount of time the producer will wait before
                 sending a batch of messages to the server.
           ---------------------------------------------------------------------------------------------------------

We will use ProduceChannel() for producing asynchronously and will get delivery report in separate go-routine by kafka.Event() channel

socket.timeout.ms => is a configuration property in the Kafka producer that controls the amount of time the producer will wait for a
                     response from the server before timing out.The default value for socket.timeout.ms is 30 seconds.
                     if timeout happens kafka will retry to send the msg in this case you might get 2 messages so keep timeout.ms is little high

*/

type ProducerConfig struct {
	AppEnv                             string
	Name                               string
	Brokers                            string
	ClientID                           string
	SASLMechanism                      string
	SASLUser                           string
	SASLPassword                       string
	EnableSslCertificationVerification bool
	SocketTimeOut                      time.Duration
	BatchNumMessages                   int
	BatchSize                          int
	LingerMs                           int
	SecurityProtocol                   string
}

type producer struct {
	asyncProducer *kafka.Producer
	config        ProducerConfig
	initialized   bool
	err           error
}

var producers = make(map[string]*producer)

func KafkaProducer(config ProducerConfig) error {
	logger := zerolog.With().Str("component", "kafka_producer").Str("name", config.Name).Logger()

	if config.SocketTimeOut < 10 {
		config.SocketTimeOut = 300000
		logger.Info().Int64("socket_timeout", int64(config.SocketTimeOut)).Msg("Using default socket timeout")
	}
	if config.BatchSize < 1 {
		config.BatchSize = 16384
		logger.Info().Int("batch_size", config.BatchSize).Msg("Using default batch size")
	}

	kafkaConfig := &kafka.ConfigMap{
		"client.id":          config.ClientID,
		"bootstrap.servers":  config.Brokers,
		"socket.timeout.ms":  config.SocketTimeOut,
		"batch.num.messages": config.BatchNumMessages,
		"batch.size":         config.BatchSize,
		"linger.ms":          config.LingerMs,
	}

	appEnv := config.AppEnv
	if appEnv != "development" {
		kafkaConfig.SetKey("sasl.mechanisms", config.SASLMechanism)
		if config.SecurityProtocol != "" {
			kafkaConfig.SetKey("security.protocol", config.SecurityProtocol)
		} else {
			kafkaConfig.SetKey("security.protocol", "SASL_SSL")
		}
		kafkaConfig.SetKey("sasl.username", config.SASLUser)
		kafkaConfig.SetKey("sasl.password", config.SASLPassword)
		kafkaConfig.SetKey("enable.ssl.certificate.verification", config.EnableSslCertificationVerification)
		logger.Info().
			Str("sasl_mechanism", config.SASLMechanism).
			Str("security_protocol", config.SecurityProtocol).
			Bool("ssl_verification", config.EnableSslCertificationVerification).
			Msg("Configured secure connection")
	}

	producers[config.Name] = &producer{
		asyncProducer: nil,
		initialized:   false,
		err:           nil,
		config:        config,
	}

	var err error
	producers[config.Name].asyncProducer, err = kafka.NewProducer(kafkaConfig)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to create Kafka producer")
		producers[config.Name].err = err
		return err
	}

	producers[config.Name].initialized = true
	producers[config.Name].err = nil
	logger.Info().Msg("Kafka producer connected successfully")

	// Start the event handler goroutine to process delivery reports and other events
	go deliveryTestHandler(producers[config.Name].asyncProducer.Events())

	return nil
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
		if env != "production" {
			NumPartitions = 1
		}

		var topicSpecs []kafka.TopicSpecification

		for _, topic := range topics {
			topicSpecs = append(topicSpecs, kafka.TopicSpecification{
				Topic:         topic,
				NumPartitions: NumPartitions,
			})
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

		// Use Produce() instead of ProduceChannel() as recommended in v2
		err := producer.asyncProducer.Produce(msg, nil)
		if err != nil {
			zerolog.Error().Err(err).Str("topic", topic).Str("producer", name).Msg("Failed to produce message")
		}
	} else {
		zerolog.Error().Str("producer", name).Msg("Producer not found")
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

		// Use Produce() instead of ProduceChannel() as recommended in v2
		err := producer.asyncProducer.Produce(msg, nil)
		if err != nil {
			zerolog.Error().Err(err).Str("topic", topic).Str("producer", name).Msg("Failed to produce message")
		}
	} else {
		zerolog.Error().Str("producer", name).Msg("Producer not found")
	}
}

func CloseProducer(name string) {
	logger := zerolog.With().Str("component", "kafka_producer").Str("name", name).Logger()

	if producer, found := producers[name]; found {
		if !producer.initialized {
			logger.Error().Msg("Kafka producer not initialized")
			return
		}

		logger.Info().Msg("Closing Kafka producer")
		producer.initialized = false
		producer.asyncProducer.Close()
		logger.Info().Msg("Kafka producer closed successfully")
	} else {
		logger.Error().Msg("Producer not found")
	}
}

func FlushProducerChannel(name string) error {
	logger := zerolog.With().Str("component", "kafka_producer").Str("name", name).Logger()

	if producer, found := producers[name]; found {
		if !producer.initialized {
			err := fmt.Errorf("kafka producer %s not initialized", name)
			logger.Error().Err(err).Msg("Cannot flush")
			return err
		}

		// Flush is still available in v2 and works with both Produce() and ProduceChannel()
		// The parameter is a timeout in milliseconds
		count := producer.asyncProducer.Flush(3000)
		if count > 0 {
			logger.Warn().Int("unflushed_count", count).Msg("Some messages remain unflushed after timeout")
		} else {
			logger.Info().Msg("All messages flushed successfully")
		}
		return nil
	} else {
		err := fmt.Errorf("producer %s not found", name)
		logger.Error().Err(err).Msg("Cannot flush")
		return err
	}
}

func deliveryTestHandler(deliveryChan chan kafka.Event) {
	logger := zerolog.With().Str("component", "kafka_delivery_handler").Logger()

	for ev := range deliveryChan {
		switch e := ev.(type) {
		case *kafka.Message:
			// Extract topic name from TopicPartition for better logging context
			topic := "unknown"
			if e.TopicPartition.Topic != nil {
				topic = *e.TopicPartition.Topic
			}

			// Build logger with context
			msgLogger := logger.With().
				Str("topic", topic).
				Int32("partition", e.TopicPartition.Partition).
				Int64("offset", int64(e.TopicPartition.Offset)).
				Logger()

			if e.TopicPartition.Error != nil {
				// Log error with context
				msgLogger.Error().
					Err(e.TopicPartition.Error).
					Msg("Failed to deliver message")
			} else {
				// Log success with context
				msgLogger.Debug().Msg("Message delivered successfully")
			}

		case kafka.Error:
			// Handle Kafka errors
			logger.Error().
				Int("error_code", int(e.Code())).
				Str("error", e.Error()).
				Msg("Kafka error occurred")

		default:
			// Log unexpected event types
			logger.Debug().
				Str("event_type", fmt.Sprintf("%T", e)).
				Msg("Received unknown event type")
		}
	}

	logger.Warn().Msg("Delivery handler exiting - event channel closed")
}
