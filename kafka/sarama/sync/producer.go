package sync

import (
	"log"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	zerolog "github.com/rs/zerolog/log"
)

var syncProducer sarama.SyncProducer
var initialized = false
var err error

type KafkaConfig struct {
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

func KafkaProducer(config KafkaConfig) {
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

	syncProducer, err = sarama.NewSyncProducer(strings.Split(config.Brokers, ","), kafkaConfig)
	if err != nil {
		zerolog.Error().Msgf(err.Error())
	} else {
		zerolog.Info().Msgf("Kafka connected")
		initialized = true
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
	p, o, err := syncProducer.SendMessage(msg)
	if err != nil {
		zerolog.Error().Msgf("Error publish: ", err.Error())
	}
	zerolog.Log().Msgf("Partition: %s ,Offset: %s", p, o)
}

func PushJSONMessages(m [][]byte, topic string) {
	if !initialized {
		zerolog.Error().Msgf("Kafka not initialized")
		return
	}
	var msgs []*sarama.ProducerMessage
	for _, val := range m {
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(val),
		}
		msgs = append(msgs, msg)
	}
	err := syncProducer.SendMessages(msgs)
	if err != nil {
		zerolog.Error().Msgf("Error publish: ", err.Error())
	}
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
	p, o, err := syncProducer.SendMessage(msg)
	if err != nil {
		zerolog.Error().Msgf("Error publish: ", err.Error())
	}
	zerolog.Log().Msgf("Partition: %s ,Offset: %s", p, o)
}

func PushStringMessages(m []string, topic string) {
	if !initialized {
		zerolog.Error().Msgf("Kafka not initialized")
		return
	}
	var msgs []*sarama.ProducerMessage
	for _, val := range m {
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(val),
		}
		msgs = append(msgs, msg)
	}
	err := syncProducer.SendMessages(msgs)
	if err != nil {
		zerolog.Error().Msgf("Error publish: ", err.Error())
	}
}

func CloseProducer() {
	if !initialized {
		zerolog.Error().Msgf("Kafka not initialized")
		return
	}
	syncProducer.Close()
}
