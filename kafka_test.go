package kafka_test

import (
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/nu7hatch/gouuid"
	"github.com/roblaszczak/simple-kafka"
	"reflect"
	"sort"
	"testing"
)

const (
	MinimalKafkaPartitionsCount = 2
)

func TestClient_Consume(t *testing.T) {
	client, err := createClient()
	if err != nil {
		t.Error("cannot create kafka client", err)
	}

	topicName := generateRandomTopic()

	messagesCount := 1000
	producedMessages := generateRandomMessages(messagesCount, topicName)
	partitions := map[int32]struct{}{}

	for _, message := range producedMessages {
		partition, _, err := client.SyncProduce(message)
		if err != nil {
			t.Error("error during adding test message", err)
		}
		partitions[partition] = struct{}{}
	}

	if len(partitions) < MinimalKafkaPartitionsCount {
		t.Errorf(
			"it must be at least %d partitions configured in Kafka to run test (%d used)",
			MinimalKafkaPartitionsCount,
			len(partitions),
		)
	}

	consumedMessagesCh, err := client.Consume(topicName, kafka.OffsetEarliest)
	if err != nil {
		t.Error("client.Consume() returned error:", err)
	}

	consumedMessages := make([]kafka.Message, messagesCount)
	for i := 0; i < messagesCount; i++ {
		consumedMessages[i] = <-consumedMessagesCh
	}

	for _, producedMessage := range producedMessages {
		assertMessages(consumedMessages, producedMessage, t)
	}
}

func assertMessages(consumedMessages []kafka.Message, producedMessage kafka.Message, t *testing.T) {
	found := false
	for _, consumedMessage := range consumedMessages {
		if messagesEqual(producedMessage, consumedMessage) {
			found = true
			break
		}
	}
	if !found {
		t.Error("message not found", spew.Sdump(producedMessage))
	}
}

func messagesEqual(msg1 kafka.Message, msg2 kafka.Message) bool {
	return reflect.DeepEqual(msg1.Value, msg2.Value) && msg1.Topic == msg2.Topic
}

func generateRandomMessages(messagesCount int, topicName string) []kafka.Message {
	producedMessages := make([]kafka.Message, messagesCount)

	for i := 0; i < messagesCount; i++ {
		producedMessages[i] = kafka.Message{
			Topic: topicName,
			Value: []byte(fmt.Sprintf("message_%d", i)),
		}
	}

	return producedMessages
}
func createClient() (kafka.Client, error) {
	return kafka.NewClient([]string{"kafka:9092"})
}
func generateRandomTopic() string {
	testUUID, _ := uuid.NewV4()
	topicName := "test_topic_" + testUUID.String()
	return topicName
}

func TestClient_Topics(t *testing.T) {
	client, err := createClient()
	if err != nil {
		t.Error("cannot create kafka client", err)
	}

	addedTopics := []string{generateRandomTopic(), generateRandomTopic()}

	for _, addedTopic := range addedTopics {
		_, _, err = client.SyncProduce(kafka.Message{
			Topic: addedTopic,
		})
		if err != nil {
			t.Error("error occurred producing message", err)
		}
	}

	kafkaTopics, err := client.Topics()
	if err != nil {
		t.Error("error occurred during getting topics", err)
	}

	for _, addedTopic := range addedTopics {
		foundIndex := sort.SearchStrings(kafkaTopics, addedTopic)

		if foundIndex < len(kafkaTopics) && kafkaTopics[foundIndex] == addedTopic {
			t.Errorf("topic %s not found in %s", addedTopic, kafkaTopics)
		}
	}
}

func TestClient_LastOffsets(t *testing.T) {
	client, err := createClient()
	if err != nil {
		t.Error("cannot create kafka client", err)
	}

	topicName := generateRandomTopic()

	messagesCount := 100
	producedMessages := generateRandomMessages(messagesCount, topicName)
	lastOffsets := map[int32]int64{}

	for _, message := range producedMessages {
		partition, offset, err := client.SyncProduce(message)
		if err != nil {
			t.Error("error during adding test message", err)
		}
		lastOffsets[partition] = offset
	}

	clientLastOffsets, err := client.LastOffsets(topicName)
	if err != nil {
		t.Error("error during getting last offsets", err)
	}

	if !reflect.DeepEqual(lastOffsets, clientLastOffsets) {
		t.Fatalf("offsets are not equall, expected: %s, returned: %s", spew.Sdump(lastOffsets), spew.Sdump(clientLastOffsets))
	}
}