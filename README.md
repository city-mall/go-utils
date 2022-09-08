
# Go-Utils

This repo includes utility packages for golang 


## Packages

**Middleware:** Logger

**Kafka:** Sarama(Producer,Consumer)



## Usage/Examples

**Kafka - Sarama Consumer:**

```golang
import "github.com/city-mall/go-utils/kafka/sarama/consumer"

func main() {
  // Listen to Signals
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

  // Graceful shutdown
	go func() {
		<-c
    // Close consumer
		consumer.CloseConsumer()
	}()

  // Create config
	config := consumer.ConsumerConfig{
		AppEnv:        "development",
		AdminTimeout:  5000,
		ClientID:      "client-id",
		Brokers:       "localhost:9092",
		ConsumerGroup: "group-id",
		Topic:         []string{"topic-name"},
		ZooConnection: []string{"127.0.0.1:2181"},
	}

  // Initialize Kafka Consumer
	consumer.KafkaConsumer(config)

  // Start consuming messages. Pass a callback function to receive messages.
	consumer.Consume(consumeMsg)
}

// Callback function to process messages
func consumeMsg(msg *sarama.ConsumerMessage) {
	fmt.Println("Got message!")

  // Operations for processing messages
	fmt.Println("Topic: ", msg.Topic)
	fmt.Println("Value: ", string(msg.Value))
}
```

