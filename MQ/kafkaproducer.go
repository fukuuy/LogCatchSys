package MQ

import (
	"github.com/IBM/sarama"
)

type KafkaProducer struct {
	Producer sarama.SyncProducer
}

func (kp *KafkaProducer) Init(broker string) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true

	var err error
	kp.Producer, err = sarama.NewSyncProducer([]string{broker}, config)
	if err != nil {
		return
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
	_, _, _ = kp.Producer.SendMessage(msg)
}
