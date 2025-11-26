package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	MainQueue = "weather-data"
	DeadQueue = "weather-data.dlq"

	OllamaURL    = "http://localhost:11434/api/generate"
	WeatherAPI   = "http://localhost:8080/api/weather"
	OllamaModel  = "mistral" // ou llama3, qwen, etc.
	ReconnectWait = 5 * time.Second
)

type WeatherInsightRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
}

type OllamaResponse struct {
	Response string `json:"response"`
}

func failToDLQ(ch *amqp.Channel, body []byte) {
	ch.Publish(
		"", DeadQueue,
		false, false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	fmt.Println("[WARN] Message sent to DLQ")
}

func callOllama(msg []byte) (string, error) {
	req := WeatherInsightRequest{
		Model:  OllamaModel,
		Prompt: fmt.Sprintf("Analyze this weather data and extract insights:\n%s", string(msg)),
	}

	buffer, _ := json.Marshal(req)
	resp, err := http.Post(OllamaURL, "application/json", bytes.NewBuffer(buffer))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)

	var result OllamaResponse
	err = json.Unmarshal(body, &result)
	if err != nil {
		return "", err
	}

	return result.Response, nil
}

func sendToWeatherAPI(insight string) error {
	payload := map[string]string{
		"insight": insight,
	}

	jsonPayload, _ := json.Marshal(payload)

	resp, err := http.Post(WeatherAPI, "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("weather-api returned status %d", resp.StatusCode)
	}

	return nil
}

func processMessage(ch *amqp.Channel, msg amqp.Delivery) {
	fmt.Println("Received:", string(msg.Body))

	// 1 → Ask Ollama for insights
	insight, err := callOllama(msg.Body)
	if err != nil {
		fmt.Println("[ERROR] Ollama processing failed:", err)
		failToDLQ(ch, msg.Body)
		msg.Ack(false)
		return
	}

	fmt.Println("Insight:", insight)

	// 2 → Send insights to weather-api
	err = sendToWeatherAPI(insight)
	if err != nil {
		fmt.Println("[ERROR] Failed to send to weather-api:", err)
		failToDLQ(ch, msg.Body)
		msg.Ack(false)
		return
	}

	// Everything succeeded
	msg.Ack(false)
	fmt.Println("[OK] Message processed successfully")
}

func connectRabbit() *amqp.Connection {
	url := os.Getenv("CLOUDAMQP_URL")
	if url == "" {
		url = "amqp://guest:guest@localhost:5672"
	}

	for {
		conn, err := amqp.Dial(url)
		if err == nil {
			return conn
		}

		fmt.Println("[ERROR] Failed to connect to RabbitMQ. Retrying in 5 seconds...")
		time.Sleep(ReconnectWait)
	}
}

func main() {
	conn := connectRabbit()
	defer conn.Close()

	ch, _ := conn.Channel()
	ch.QueueDeclare(MainQueue, true, false, false, false, nil)
	ch.QueueDeclare(DeadQueue, true, false, false, false, nil)

	msgs, err := ch.Consume(
		MainQueue,
		"",
		false, false, false, false, nil,
	)

	if err != nil {
		panic(err)
	}

	fmt.Println("Listening on queue:", MainQueue)

	for msg := range msgs {
		processMessage(ch, msg)
	}
}
