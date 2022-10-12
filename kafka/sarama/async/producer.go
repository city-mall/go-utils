package async

import (
	"log"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	zerolog "github.com/rs/zerolog/log"
)

var (
	asyncProducer sarama.AsyncProducer
	initialized   = false
	err           error
)

type ProducerConfig struct {
	AppEnv          string
	ProducerTimeout time.Duration
	AdminTimeout    time.Duration
	ClientID        string
	SASLEnable      bool
	SASLMechanism   sarama.SASLMechanism
	SASLUser        string
	SASLPassword    string
	Brokers         string
}

func KafkaProducer(config ProducerConfig) {
	sarama.Logger = log.New(os.Stdout, "", log.Ltime)

	kafkaConfig := sarama.NewConfig()

	appEnv := config.AppEnv
	kafkaConfig.Producer.Timeout = config.ProducerTimeout
	kafkaConfig.ClientID = config.ClientID
	kafkaConfig.Admin.Timeout = config.AdminTimeout
	if appEnv != "development" {
		kafkaConfig.Net.SASL.Enable = config.SASLEnable
		kafkaConfig.Net.SASL.Mechanism = config.SASLMechanism
		kafkaConfig.Net.SASL.User = config.SASLUser
		kafkaConfig.Net.SASL.Password = config.SASLPassword
	}

	asyncProducer, err = sarama.NewAsyncProducer(strings.Split(config.Brokers, ","), kafkaConfig)
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
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(m),
	}
	asyncProducer.Input() <- msg
}

func PushStringMessage(m string, topic string) {
	if !initialized {
		zerolog.Error().Msgf("Kafka not initialized")
		return
	}
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(m),
	}
	asyncProducer.Input() <- msg
}

func CloseProducer() {
	if !initialized {
		zerolog.Error().Msgf("Kafka not initialized")
		return
	}
	asyncProducer.Close()
}
