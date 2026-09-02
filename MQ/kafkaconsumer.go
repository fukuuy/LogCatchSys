package MQ

import (
	"github.com/IBM/sarama"
)

type KafkaConsumer struct {
	Consumer sarama.Consumer
}

func (kc *KafkaConsumer) Init(broker string) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	
}