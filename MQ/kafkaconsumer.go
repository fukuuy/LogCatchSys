package MQ

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
)

// OnMessage 回调，负责处理消费到的 kafka 消息（key 与 value）。
type OnMessage func(key []byte, value []byte)

// KafkaConsumer 使用 ConsumerGroup 消费 kafka 消息，支持水平扩展与 offset 自动管理。
type KafkaConsumer struct {
	brokers  []string
	groupID  string
	topics   []string
	group    sarama.ConsumerGroup
	onMessage OnMessage
}

// groupHandler 实现 sarama.ConsumerGroupHandler 接口。
type groupHandler struct {
	onMessage OnMessage
}

func (h *groupHandler) Setup(s sarama.ConsumerGroupSession) error {
	return nil
}

func (h *groupHandler) Cleanup(s sarama.ConsumerGroupSession) error {
	return nil
}

func (h *groupHandler) ConsumeClaim(s sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			if h.onMessage != nil {
				h.onMessage(msg.Key, msg.Value)
			}
			// 处理完成后标记，提交 offset
			s.MarkMessage(msg, "")
		case <-s.Context().Done():
			return nil
		}
	}
}

// Init 创建 ConsumerGroup。
func (kc *KafkaConsumer) Init(brokers []string, groupID string, topics []string) error {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	client, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		fmt.Printf("Failed to start Sarama consumer group: %v\n", err)
		return err
	}

	kc.brokers = brokers
	kc.groupID = groupID
	kc.topics = topics
	kc.group = client
	return nil
}

// SetOnMessage 设置消息处理回调。
func (kc *KafkaConsumer) SetOnMessage(fn OnMessage) {
	kc.onMessage = fn
}

// Run 持续消费，直到 ctx 被取消。消费组重平衡错误时记录并继续。
func (kc *KafkaConsumer) Run(ctx context.Context) error {
	handler := &groupHandler{onMessage: kc.onMessage}
	for {
		if err := kc.group.Consume(ctx, kc.topics, handler); err != nil {
			// 正常退出
			if ctx.Err() != nil {
				return nil
			}
			fmt.Printf("kafka consumer group error: %v\n", err)
			continue
		}
		// Consume 返回后若 ctx 未取消则重新平衡，循环继续
		if ctx.Err() != nil {
			return nil
		}
	}
}

// Close 关闭消费组 client。
func (kc *KafkaConsumer) Close() {
	if kc.group != nil {
		if err := kc.group.Close(); err != nil {
			fmt.Printf("close kafka consumer group err: %v\n", err)
		}
	}
}
