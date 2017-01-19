package kafka

import (
	"github.com/Shopify/sarama"
)

// Message represents message received from Kafka
type Message struct {
	Topic string
	Value []byte
}

// Client is Kafka client.
type Client interface {
	Topics() ([]string, error)
	Consume(topic string) (<-chan Message, error)
	SyncProduce(Message) (partition int32, offset int64, err error)
	// AsyncProduce(Message) error - todo
}

// NewClient creates new Client.
func NewClient(brokers []string) (Client, error) {
	return &client{brokers: brokers}, nil
}

type client struct {
	brokers            []string
	saramaConsumer     sarama.Consumer
	saramaSyncProducer sarama.SyncProducer
}

func (c *client) consumer() (sarama.Consumer, error) {
	if c.saramaConsumer == nil {
		print("creating consumer")

		consumer, err := sarama.NewConsumer(c.brokers, nil)
		if err != nil {
			return nil, err
		}

		c.saramaConsumer = consumer
	}

	return c.saramaConsumer, nil
}

func (c *client) syncProducer() (sarama.SyncProducer, error) {
	if c.saramaSyncProducer == nil {
		print("creating producer")

		producer, err := sarama.NewSyncProducer(c.brokers, nil)
		if err != nil {
			return nil, err
		}

		c.saramaSyncProducer = producer
	}

	return c.saramaSyncProducer, nil
}

func (c client) Topics() ([]string, error) {
	consumer, err := c.consumer()

	if err != nil {
		return []string{}, err
	}

	return consumer.Topics()
}

func (c *client) Consume(topic string) (<-chan Message, error) {
	consumer, err := c.consumer()
	if err != nil {
		return nil, err
	}

	partitionList, err := consumer.Partitions(topic)
	if err != nil {
		return nil, err
	}

	messages := make(chan Message)

	for _, partition := range partitionList {
		pc, err := consumer.ConsumePartition(topic, partition, 0)
		if err != nil {
			return nil, err
		}

		go func(pc sarama.PartitionConsumer, partition int32) {
			for message := range pc.Messages() {
				println("waiting for partition", partition)
				messages <- Message{
					Value: message.Value,
					Topic: topic,
				}
				println("consumed from partition", partition)
			}
		}(pc, partition)
	}

	return messages, nil
}

func (c *client) SyncProduce(message Message) (partition int32, offset int64, err error) {
	producer, err := c.syncProducer()
	if err != nil {
		return -1, -1, err
	}

	producerMessage := sarama.ProducerMessage{
		Topic: message.Topic,
		Value: sarama.ByteEncoder(message.Value),
	}

	println("producing...")

	return producer.SendMessage(&producerMessage)
}
