package consumer

import (
	"log"
	"os"
	"time"

	"github.com/Shopify/sarama"
	zerolog "github.com/rs/zerolog/log"
	"github.com/wvanbergen/kafka/consumergroup"
)

var (
	consumer    *consumergroup.ConsumerGroup
	initialized = false
	err         error
)

type ConsumerConfig struct {
	AppEnv        string
	AdminTimeout  time.Duration
	ClientID      string
	SASLEnable    bool
	SASLMechanism sarama.SASLMechanism
	SASLUser      string
	SASLPassword  string
	Brokers       string
	ConsumerGroup string
	Topic         []string
	ZooConnection []string
}

func KafkaConsumer(config ConsumerConfig) {
	sarama.Logger = log.New(os.Stdout, "", log.Ltime)

	appEnv := config.AppEnv
	kafkaConfig := consumergroup.NewConfig()
	kafkaConfig.Config.ClientID = config.ClientID
	kafkaConfig.Config.Admin.Timeout = config.AdminTimeout
	kafkaConfig.Config.Metadata.Retry.Max = 10

	if appEnv != "development" {
		kafkaConfig.Config.Net.SASL.Enable = config.SASLEnable
		kafkaConfig.Config.Net.SASL.Mechanism = config.SASLMechanism
		kafkaConfig.Config.Net.SASL.User = config.SASLUser
		kafkaConfig.Config.Net.SASL.Password = config.SASLPassword
	}

	kafkaConfig.Offsets.Initial = sarama.OffsetOldest
	kafkaConfig.Offsets.ProcessingTimeout = 10 * time.Second

	consumer, err = consumergroup.JoinConsumerGroup(config.ConsumerGroup, config.Topic, config.ZooConnection, kafkaConfig)
	if err != nil {
		zerolog.Error().Msgf(err.Error())
	}
	initialized = true
	zerolog.Info().Msgf("Consumer connected")
}

func Consume(callback func(*sarama.ConsumerMessage)) {
	if !initialized {
		zerolog.Error().Msgf("Consumer not initialized")
		return
	}
	go func() {
		for err := range consumer.Errors() {
			zerolog.Info().Msgf(err.Error())
		}
	}()
	for {
		select {
		case msg := <-consumer.Messages():
			if msg == nil {
				return
			}
			callback(msg)

			err := consumer.CommitUpto(msg)
			if err != nil {
				zerolog.Info().Msgf("Error commit zookeeper: ", err.Error())
			}
		}
	}
}

func CloseConsumer() {
	if !initialized {
		zerolog.Error().Msgf("Consumer not initialized")
		return
	}
	consumer.Close()
}
