package kafka

import (
	"github.com/Shopify/sarama"
	"io/ioutil"
	"log"
)

var (
	Logger = log.New(ioutil.Discard, "[simple-kafka] ", log.LstdFlags)
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
	sarama.Logger = Logger
	return &client{brokers: brokers}, nil
}

type client struct {
	brokers            []string
	saramaConsumer     sarama.Consumer
	saramaSyncProducer sarama.SyncProducer
}

func (c client) saramaConfig() (*sarama.Config) {
	config := sarama.NewConfig()
	return config
}

func (c *client) consumer() (sarama.Consumer, error) {
	if c.saramaConsumer == nil {
		Logger.Print("creating consumer")

		consumer, err := sarama.NewConsumer(c.brokers, c.saramaConfig())
		if err != nil {
			return nil, err
		}

		c.saramaConsumer = consumer
	}

	return c.saramaConsumer, nil
}

func (c *client) syncProducer() (sarama.SyncProducer, error) {
	if c.saramaSyncProducer == nil {
		Logger.Print("creating producer")

		config := c.saramaConfig()
		config.Producer.Return.Successes = true
		config.Producer.Return.Errors = true

		producer, err := sarama.NewSyncProducer(c.brokers, config)
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
		pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
		if err != nil {
			return nil, err
		}

		go func(pc sarama.PartitionConsumer, partition int32) {
			Logger.Printf("intializing consumer for partition %d", partition)

			for message := range pc.Messages() {
				Logger.Printf("waiting for partition %d", partition)
				messages <- Message{
					Value: message.Value,
					Topic: topic,
				}
				Logger.Printf("consumed from partition %d", partition)
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

	Logger.Print("producing message to topic %s", message.Topic)

	return producer.SendMessage(&producerMessage)
}
