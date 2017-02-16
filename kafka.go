package kafka

import (
	"github.com/Shopify/sarama"
	"io/ioutil"
	"log"
)

var (
	// Logger is logger used by simple-kafka.
	// Logger can be replaced by any other logger.
	Logger = log.New(ioutil.Discard, "[simple-kafka] ", log.LstdFlags)
)

// Message represents message received from Kafka
type Message struct {
	Topic  string
	Value  []byte
	Offset int64
	Partition int32
}

const (
	// OffsetLastest can be used to consume only lastests messages.
	OffsetLastest  int64 = -1
	// OffsetEarliest can be used to consume all messages.
 	OffsetEarliest int64 = -2
)

// Client is Kafka client.
type Client interface {
	// Topics returns list of existing topics.
	Topics() ([]string, error)

	// Consume creates channel with provides messages from provided topic.
	Consume(topic string, offset int64) (<-chan Message, error)

	// SyncProduce produces provided message.
	SyncProduce(Message) (partition int32, offset int64, err error)

	// LastOffsets returns last offsets for all partitions of provided topic.
	LastOffsets(topic string) (map[int32]int64, error)

	// AsyncProduce(Message) error - todo
}

// NewClient creates new Client.
func NewClient(brokers []string) (Client, error) {
	sarama.Logger = Logger
	return &client{brokers: brokers}, nil
}

type client struct {
	brokers            []string
	saramaSyncProducer sarama.SyncProducer
	saramaClient       sarama.Client
}

func (c client) saramaConfig() (*sarama.Config) {
	config := sarama.NewConfig()
	return config
}

func (c *client) consumer() (sarama.Consumer, error) {
	Logger.Print("creating consumer")

	consumer, err := sarama.NewConsumer(c.brokers, c.saramaConfig())
	if err != nil {
		return nil, err
	}

	return consumer, nil
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

func (c *client) client() (sarama.Client, error) {
	if c.saramaClient == nil || c.saramaClient.Closed() {
		Logger.Print("creating client")

		config := c.saramaConfig()

		client, err := sarama.NewClient(c.brokers, config)
		if err != nil {
			return nil, err
		}

		c.saramaClient = client
	}

	return c.saramaClient, nil
}

func (c client) Topics() ([]string, error) {
	consumer, err := c.consumer()

	if err != nil {
		return []string{}, err
	}

	return consumer.Topics()
}

func (c *client) Consume(topic string, offset int64) (<-chan Message, error) {
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
		pc, err := consumer.ConsumePartition(topic, partition, offset)
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
					Offset: message.Offset,
					Partition: partition,
				}
				Logger.Printf("consumed from partition %d", partition)
			}
		}(pc, partition)
	}

	return messages, nil
}

func (c *client) LastOffsets(topic string) (map[int32]int64, error) {
	client, err := c.client()

	if err != nil {
		return map[int32]int64{}, err
	}

	partitions, err := client.Partitions(topic)
	if err != nil {
		return map[int32]int64{}, err
	}

	offsets := make(map[int32]int64, len(partitions))

	for _, partition := range partitions {
		nextOffset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			return map[int32]int64{}, err
		}

		// kafka returns the next offset, we want lastest
		offsets[partition] = nextOffset - 1
	}

	return offsets, nil
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

	Logger.Printf("producing message to topic %s", message.Topic)

	return producer.SendMessage(&producerMessage)
}
