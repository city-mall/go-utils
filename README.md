
# Go-Utils

This repo includes utility packages for golang 


## Packages

**Middleware:** Logger

**Kafka:** Confluent, Sarama, Segmentio(Producer,Consumer)

**DB:** Postgres, MongoDB


## Usage/Examples

**Kafka - Consumer:**

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

**Kafka - Producer:**

```golang
import "github.com/city-mall/go-utils/kafka/sarama/async"

func main() {
    // Create config
    config := async.ProducerConfig{
        AppEnv:       "development",
        ReadTimeout:  time.Second * 10,
        WriteTimeout: time.Second * 10,
        ClientID:     "client-id",
        Brokers:      "localhost:9092",
	Name:         "cm-live"
    }

    // Inititalize Kafka producer
    async.KafkaProducer(config)
    
    // Send string to kafka
    async.PushStringMessage("Hey there!", "applink-events", "cm-live")
    
    // Send JSON(Byte[]) to kafka
    async.PushJSONMessage([]byte(`{"num":6.13,"strs":["a","b"]}`), "applink-events", "cm-live")

    // Close Producer
    async.CloseProducer("cm-live")
}
```

**DB:**

**Note: MongoDB connection requires pem file to be saved in root folder './' with name 'rds-combined-ca-bundle.pem'

```golang
import "github.com/city-mall/go-utils/db/postgres"

func main() {
        // Create config
	config := postgres.Client{
		GO_ENV: "development",
		DB_URL: "postgres://cm:cm@localhost:5432/cmdb",
	}
	
	// Connect to Postgres
	postgres.Connect(config)
	
	// Fetch instance
	db, err := postgres.GetDb()

	// Check for Error
	if err != nil {
	   log.Fatal(err)
	}
	
	// Close DB Connection
	postgres.Close()
}
```
