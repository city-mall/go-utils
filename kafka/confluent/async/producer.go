package async

import (
	"context"
	"errors"
	"fmt"
	"time"

	zerolog "github.com/rs/zerolog/log"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
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
}

type producer struct {
	asyncProducer *kafka.Producer
	config        ProducerConfig
	initialized   bool
	err           error
}

var producers = make(map[string]*producer)

func KafkaProducer(config ProducerConfig) error {
	if config.SocketTimeOut < 10 {
		config.SocketTimeOut = 300000
	}
	if config.BatchSize < 1 {
		config.BatchSize = 16384
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
		return producers[config.Name].err
	} else {
		producers[config.Name].initialized = true
		zerolog.Info().Msgf("Kafka connected %v", config.Name)
		go deliveryTestHandler(producers[config.Name].asyncProducer.Events())
		return nil
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
		producer.asyncProducer.ProduceChannel() <- msg
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
		producer.asyncProducer.ProduceChannel() <- msg
	} else {
		zerolog.Error().Msgf("Producer %v not found", name)
	}
}

func CloseProducer(name string) {
	if producer, found := producers[name]; found {
		if !producer.initialized {
			zerolog.Error().Msgf("Kafka %v not initialized", name)
			return
		}
		FlushProducerChannel(name)
		zerolog.Info().Msgf("Closing %v", name)
		producer.asyncProducer.Close()
	} else {
		zerolog.Error().Msgf("Producer %v not found", name)
	}
}

func FlushProducerChannel(name string) error {
	if producer, found := producers[name]; found {
		if !producer.initialized {
			err := errors.New(fmt.Sprintf("Kafka %v not initialized", name))
			zerolog.Error().Msgf(err.Error())
			return err
		}
		count := producer.asyncProducer.Flush(3000)
		zerolog.Info().Msgf("Unflushed Messages %v", count)
		return nil
	} else {
		err := errors.New(fmt.Sprintf("Producer %v not found", name))
		zerolog.Error().Msgf(err.Error())
		return err
	}
}

func deliveryTestHandler(deliveryChan chan kafka.Event) {
	for ev := range deliveryChan {
		m, ok := ev.(*kafka.Message)
		if !ok {
			continue
		}
		if m.TopicPartition.Error != nil {
			zerolog.Debug().Msgf("Message delivery tp %v error: %v value %v", m.TopicPartition, m.TopicPartition.Error, m.Value)
		} else {
			zerolog.Debug().Msgf("Message sent success %v", m.Value)
		}
	}
}
