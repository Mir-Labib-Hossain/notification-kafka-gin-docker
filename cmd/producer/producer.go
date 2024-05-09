package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"notification-kafka-gin-docker/pkg/models"
	"strconv"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
)

const (
	ProducerPort       = ":8080"
	KafkaServerAddress = "localhost:9092"
	kafkaTopic         = "notification"
)

////////////////////////////////////////
////
////          HELPER FUNCS
////
////////////////////////////////////////

var ErrUserNotFoundInProducer = errors.New("user not found")

func findUserById(id int, users []models.User) (models.User, error) {
	for _, user := range users {
		if user.ID == id {
			return user, nil
		}
	}
	return models.User{}, ErrUserNotFoundInProducer
}

func getIdFromRequest(formValue string, ctx *gin.Context) (int, error) {
	fmt.Println(ctx.PostForm(formValue))
	id, err := strconv.Atoi(ctx.PostForm(formValue))
	if err != nil {
		return 0, fmt.Errorf("failed to parse Id from value %s: %w", formValue, err)
	}
	return id, nil
}

////////////////////////////////////////
////
////        KAFKA RELATED FUNCS
////
////////////////////////////////////////

func sendKafkaMessage(producer sarama.SyncProducer, users []models.User, ctx *gin.Context, formId int, toId int) error {
	message := ctx.PostForm("message")

	fromUser, err := findUserById(formId, users)
	if err != nil {
		return err
	}

	toUser, err := findUserById(toId, users)
	if err != nil {
		return err

	}

	notification := models.Notification{
		From:    fromUser,
		To:      toUser,
		Message: message,
	}
	notificationJSON, err := json.Marshal(notification)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: kafkaTopic,
		Key:   sarama.StringEncoder(strconv.Itoa(toUser.ID)),
		Value: sarama.StringEncoder(notificationJSON),
	}
	_, _, err = producer.SendMessage(msg)
	return err

}

func sendMessageHandler(producer sarama.SyncProducer, users []models.User) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		fromId, err := getIdFromRequest("fromId", ctx)
		if err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
			return

		}
		toId, err := getIdFromRequest("toId", ctx)
		if err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
			return
		}
		err = sendKafkaMessage(producer, users, ctx, fromId, toId)

		if errors.Is(err, ErrUserNotFoundInProducer) {
			ctx.JSON(http.StatusNotFound, gin.H{"message": "User not found"})
			return
		}

		if err != nil {
			ctx.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
			return
		}
		ctx.JSON(http.StatusOK, gin.H{"message": "Message sended successfully"})

	}
}

func setupProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{KafkaServerAddress}, config)
	if err != nil {
		return nil, fmt.Errorf("failed to setup producer %s", err)
	}
	return producer, nil
}

func main() {
	users := []models.User{
		{ID: 1, Name: "Fayek"},
		{ID: 2, Name: "Labib"},
		{ID: 3, Name: "Dua"},
		{ID: 4, Name: "Ayon"},
	}
	producer, err := setupProducer()
	if err != nil {
		log.Fatalf("Failed to initialize producer: %v", err)
	}
	defer producer.Close()
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.POST("/send", sendMessageHandler(producer, users))
	fmt.Printf("Kafka producer started at http://localhost:%s\n", ProducerPort)
	if err := router.Run(ProducerPort); err != nil {
		log.Printf("Failed to run the server %v", err)
	}
}
