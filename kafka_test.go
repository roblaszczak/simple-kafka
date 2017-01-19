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

func TestClient_Consume(t *testing.T) {
	client, err := createClient()
	if err != nil {
		t.Error("cannot create kafka client", err)
	}

	topicName := generateRandomTopic()

	messagesCount := 1000
	producedMessages := generateRandomMessages(messagesCount, topicName)

	for _, message := range producedMessages {
		_, _, err := client.SyncProduce(message)
		if err != nil {
			t.Error("error during adding test message", err)
		}
	}

	consumedMessagesCh, err := client.Consume(topicName)
	if err != nil {
		t.Error("client.Consume() returned error:", err)
	}

	consumedMessages := make([]kafka.Message, messagesCount)
	for i := 0; i < messagesCount; i++ {
		consumedMessages[i] = <-consumedMessagesCh
		fmt.Println(i, string(consumedMessages[i].Value))
	}

	for _, producedMessage := range producedMessages {
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
}
func messagesEqual(msg1 kafka.Message, msg2 kafka.Message) bool {
	return reflect.DeepEqual(msg1, msg2)
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
		foundCount := sort.SearchStrings(kafkaTopics, addedTopic)

		if foundCount == 0 {
			t.Errorf("topic %s not found in %s", addedTopic, kafkaTopics)
		}
	}
}
