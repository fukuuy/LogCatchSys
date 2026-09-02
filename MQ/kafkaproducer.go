package MQ

import (
	"fmt"
	"github.com/IBM/sarama"
)

type KafkaProducer struct {
	Producer sarama.SyncProducer
}

func (kp *KafkaProducer) Init(broker string)  {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true

	var err error
	kp.Producer, err = sarama.NewSyncProducer([]string{broker}, config)
	if err != nil {
		fmt.Printf("Failed to start Sarama producer: %v", err)
	}
}

func (kp *KafkaProducer) Close() {
	if kp.Producer != nil {
		kp.Producer.Close()
	}
}

func (kp *KafkaProducer) Send(key string, value string) {
	msg := &sarama.ProducerMessage{
		Topic: "logcatchsys",
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(value),
	}
	partition, offset, err := kp.Producer.SendMessage(msg)
	if err != nil {
		fmt.Printf("Failed to send message: %v", err)
		return
	}
	fmt.Printf("Message sent to partition %d at offset %d\n", partition, offset)
}