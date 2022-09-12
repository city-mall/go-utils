package async

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
	asyncProducer *kafka.Writer
	initialized   = false
	err           error
)

type ProducerConfig struct {
	AppEnv        string
	ReadTimeout   time.Duration
	WriteTimeout  time.Duration
	ClientID      string
	SASLEnable    bool
	SASLMechanism sasl.Mechanism
	SASLUser      string
	SASLPassword  string
	Brokers       string
}

func KafkaProducer(config ProducerConfig) {
	appEnv := config.AppEnv
	asyncProducer = &kafka.Writer{
		Addr:                   kafka.TCP(strings.Split(config.Brokers, ",")...),
		Async:                  true,
		ReadTimeout:            config.ReadTimeout,
		WriteTimeout:           config.WriteTimeout,
		Logger:                 log.New(os.Stdout, "", log.Ltime),
		AllowAutoTopicCreation: true,
	}
	if appEnv != "development" {
		var sharedTransport *kafka.Transport

		if config.SASLEnable {
			if config.SASLMechanism.Name() == scram.SHA512.Name() {
				mechanism, err := scram.Mechanism(scram.SHA512, config.SASLUser, config.SASLPassword)
				if err != nil {
					panic(err)
				}
				sharedTransport = &kafka.Transport{
					SASL: mechanism,
				}
			} else if config.SASLMechanism.Name() == "PLAIN" {
				mechanism := plain.Mechanism{
					Username: config.SASLUser,
					Password: config.SASLPassword,
				}
				sharedTransport = &kafka.Transport{
					SASL: mechanism,
				}
			} else {
				log.Fatalln("Invalid SASL Mechanism")
				return
			}
		}

		sharedTransport.ClientID = config.ClientID

		asyncProducer.Transport = sharedTransport
	}
	initialized = true
	zerolog.Info().Msgf("Kafka connected")
}

func PushJSONMessage(m []byte, topic string) {
	err := asyncProducer.WriteMessages(context.Background(), kafka.Message{
		Topic: topic,
		Value: m,
	})
	if err != nil {
		zerolog.Error().Msgf("Error in pushing message to kafka", err)
	}
}

func PushStringMessage(m string, topic string) {
	err := asyncProducer.WriteMessages(context.Background(), kafka.Message{
		Topic: topic,
		Value: []byte(m),
	})
	zerolog.Log().Msgf("Message pushed")
	if err != nil {
		zerolog.Error().Msgf("Error in pushing message to kafka", err)
	}
}

func CloseProducer() {
	if !initialized {
		zerolog.Error().Msgf("Kafka not initialized")
		return
	}
	err := asyncProducer.Close()
	if err != nil {
		zerolog.Error().Msgf("Error in closing kafka producer connection", err)
	}
}
