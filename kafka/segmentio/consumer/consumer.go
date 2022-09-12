package consumer

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	zerolog "github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

var (
	consumer    *kafka.Reader
	initialized = false
	err         error
)

type ConsumerConfig struct {
	AppEnv        string
	ClientID      string
	SASLEnable    bool
	SASLMechanism sasl.Mechanism
	SASLUser      string
	SASLPassword  string
	ConsumerGroup string
	Topic         []string
	Brokers       string
}

func KafkaConsumer(config ConsumerConfig) {
	appEnv := config.AppEnv
	kafkaConfig := kafka.ReaderConfig{
		Brokers:     strings.Split(config.Brokers, ","),
		GroupTopics: config.Topic,
		GroupID:     config.ConsumerGroup,
		Logger:      log.New(os.Stdout, "", log.Ltime),
	}

	if appEnv != "development" {
		var dialer *kafka.Dialer

		if config.SASLEnable {
			if config.SASLMechanism.Name() == scram.SHA512.Name() {
				mechanism, err := scram.Mechanism(scram.SHA512, config.SASLUser, config.SASLPassword)
				if err != nil {
					panic(err)
				}
				dialer = &kafka.Dialer{
					Timeout:       10 * time.Second,
					DualStack:     true,
					SASLMechanism: mechanism,
				}
			} else if config.SASLMechanism.Name() == "PLAIN" {
				mechanism := plain.Mechanism{
					Username: config.SASLUser,
					Password: config.SASLPassword,
				}
				dialer = &kafka.Dialer{
					Timeout:       10 * time.Second,
					DualStack:     true,
					SASLMechanism: mechanism,
				}
			} else {
				log.Fatalln("Invalid SASL Mechanism")
				return
			}
		}

		dialer.ClientID = config.ClientID

		kafkaConfig.Dialer = dialer
	}

	consumer = kafka.NewReader(kafkaConfig)

	initialized = true
	zerolog.Info().Msgf("Consumer connected")
}

func Consume(callback func(kafka.Message)) {
	if !initialized {
		zerolog.Error().Msgf("Consumer not initialized")
		return
	}

	for {
		m, err := consumer.ReadMessage(context.Background())
		if err != nil {
			zerolog.Info().Msgf("Error reading messages: ", err.Error())
			return
		}
		callback(m)
	}
}

func CloseConsumer() {
	if !initialized {
		zerolog.Error().Msgf("Consumer not initialized")
		return
	}
	consumer.Close()
}
