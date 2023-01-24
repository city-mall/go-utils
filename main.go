package main

import (
	"fmt"
)

func main() {
	fmt.Println("Welcome to Go-Utils")
	//kafkaConfig := async.ProducerConfig{
	//	AppEnv:           "development",
	//	Name:             "test",
	//	Brokers:          "localhost:9092",
	//	ClientID:         "clid",
	//	BatchNumMessages: 11,
	//	LingerMs:         100000,
	//	SocketTimeOut:    0,
	//}
	//
	//fmt.Println(async.KafkaProducer(kafkaConfig))
	//async.CreateTopic([]string{"testTp"}, "development", "test")
	//time.Sleep(2 * time.Second)
	//for i := 0; i < 100; i++ {
	//	async.PushJSONMessage([]byte("yyyyyyyy"), "testTp", "test")
	//	fmt.Println("send")
	//}
	//async.CloseProducer("test")

}
