package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"notification-kafka-gin-docker/pkg/models"
	"sync"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
)

const (
	ConsumerGroup      = "notification-group"
	ConsumerTopic      = "notifications"
	ConsumerPort       = ":8081"
	KafkaServerAddress = "localhost:9092"
)

////////////////////////////////////////
////
////          HELPER FUNCS
////
////////////////////////////////////////

var ErrorNoMessageFound = errors.New("no messages found")

func getUserIdFromRequest(ctx *gin.Context) (string, error) {
	userId := ctx.Param("userId")
	if userId == "" {
		return "", ErrorNoMessageFound

	}
	return userId, nil
}

////////////////////////////////////////
////
////       NOTIFICATION STORAGE
////
////////////////////////////////////////

type UserNotifications map[string][]models.Notification

type NotificationStore struct {
	data UserNotifications
	mu   sync.RWMutex
}

func (ns *NotificationStore) Add(userId string, notification models.Notification) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.data[userId] = append(ns.data[userId], notification)
}

func (ns *NotificationStore) Get(userId string) []models.Notification {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	return ns.data[userId]
}

////////////////////////////////////////
////
////        KAFKA RELATED FUNCS
////
////////////////////////////////////////

type Consumer struct {
	store *NotificationStore
}

func (*Consumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (*Consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (consumer *Consumer) ConsumeClaim(
	sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		userID := string(msg.Key)
		var notification models.Notification
		err := json.Unmarshal(msg.Value, &notification)
		if err != nil {
			log.Printf("failed to unmarshal notification: %v", err)
			continue
		}
		consumer.store.Add(userID, notification)
		sess.MarkMessage(msg, "")
	}
	return nil
}
func initializeConsumerGroup() (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()
	consumerGroup, err := sarama.NewConsumerGroup([]string{KafkaServerAddress}, ConsumerGroup, config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize consumer group: %w", err)
	}
	return consumerGroup, nil
}
func setupConsumerGroup(ctx context.Context, store *NotificationStore) {
	consumerGroup, err := initializeConsumerGroup()
	if err != nil {
		log.Printf("initialization error: %v", err)
	}
	defer consumerGroup.Close()
	consumer := &Consumer{
		store: store,
	}

	for {
		err = consumerGroup.Consume(ctx, []string{ConsumerTopic}, consumer)
		if err != nil {
			log.Printf("error from consumer: %v", err)
		}
		if ctx.Err() != nil {
			return
		}

	}
}

func handleNotification(ctx *gin.Context, store *NotificationStore) {
	userId, err := getUserIdFromRequest(ctx)
	if err != nil {
		ctx.JSON(http.StatusNotFound, gin.H{"message": err.Error()})
		return
	}
	notes := store.Get(userId)
	if len(notes) == 0 {
		ctx.JSON(http.StatusOK,
			gin.H{
				"message":       "No notification found for user",
				"notifications": []models.Notification{},
			})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"notifications": notes})

}

func main() {
	store := &NotificationStore{
		data: make(UserNotifications),
	}
	ctx, cancel := context.WithCancel(context.Background())
	go setupConsumerGroup(ctx, store)
	defer cancel()
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.GET("/notifications/:userId", func(ctx *gin.Context) { handleNotification(ctx, store) })
	fmt.Printf("Kafka Consumer (Group %s) started at http://localhost%s", ConsumerGroup, ConsumerPort)
	if err := router.Run(ConsumerPort); err != nil {
		log.Printf("failed to run the server: %v", err)
	}
}
