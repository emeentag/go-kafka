package main

import (
	"log"
	"time"

	"github.com/Shopify/sarama"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	brokerList = kingpin.Flag("brokerList", "List of brokers to connect.").Default("node1:9092").Strings()
	topic      = kingpin.Flag("topic", "Topic name.").Default("PageView").String()
	maxRetry   = kingpin.Flag("maxRetry", "Maximum retry.").Default("3").Int()
)

func main() {
	kingpin.Parse()
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = *maxRetry
	config.Producer.Return.Successes = true
	brokers := *brokerList

	producer, err := sarama.NewSyncProducer(brokers, config)

	if err != nil {
		log.Panic(err)
	}

	defer func() {
		err := producer.Close()

		if err != nil {
			log.Panic(err)
		}
	}()

	msg := &sarama.ProducerMessage{
		Topic: *topic,
		Key:   sarama.StringEncoder("PageViewEvent"),
		Value: sarama.StringEncoder("Message produced."),
	}

	var partition int32
	var offset int64

	for i := 0; i < 5; i++ {
		partition, offset, err = producer.SendMessage(msg)
		time.Sleep(time.Second * 3)
	}

	if err != nil {
		log.Panic(err)
	}

	log.Printf("Message stored in topic(%s)/partition(%d)/offset(%d)\n", *topic, partition, offset)
}
