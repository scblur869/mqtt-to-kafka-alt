package main

/*
import (
	"context"
	"os"

	kafka "github.com/segmentio/kafka-go"
)
NOT REALLY USING ANY THING HERE
THIS IS JUST A MORE MANAGED CODE WAY OF DOING THIS


func getKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}


   *** save for TLS Implementation
   w := kafka.Writer{
       Addr: kafka.TCP("localhost:9092", "localhost:9093", "localhost:9094"),
       Topic:   "topic-A",
       Balancer: &kafka.Hash{},
       Transport: &kafka.Transport{
           TLS: &tls.Config{},
         },
       }


func WriteEvent(topic string, payload string) (string, error) {
	kafkaWriter := getKafkaWriter(os.Getenv("KAFKAURL"), topic)
	defer kafkaWriter.Close()
	msg := kafka.Message{
		Key:   []byte(topic),
		Value: []byte(payload),
	}
	err := kafkaWriter.WriteMessages(context.Background(), msg)

	return topic, err

}
*/
