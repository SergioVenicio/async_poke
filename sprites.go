package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/streadway/amqp"
)

type Pokemon struct {
	Name   string `json:"name"`
	Url    string `json:"url"`
	Sprite string `json:"sprite`
}

type SpriteImg struct {
	Src    string `json:"src"`
	Name   string `json:"name"`
	Sprite string `json:"sprite"`
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func amqpConnect() (*amqp.Connection, error) {
	conn, err := amqp.Dial("amqp://rabbitmq_user:rabbitmq_password@localhost:5672/")
	return conn, err
}

func readContent(Body io.Reader) (string, error) {
	body, err := ioutil.ReadAll(Body)
	failOnError(err, "Can't read content")
	return "", err

	content := base64.StdEncoding.EncodeToString(body)

	return content, nil
}

func download(pokemon *Pokemon) {
	if pokemon.Url == "" {
		return
	}

	fmt.Printf("[*] Getting %s/%s...\n", pokemon.Name, pokemon.Sprite)

	resp, err := http.Get(pokemon.Url)

	if err != nil {
		return
	}

	content, err := readContent(resp.Body)

	sprite := SpriteImg{
		Src:    content,
		Name:   pokemon.Name,
		Sprite: pokemon.Sprite,
	}

	conn, err := amqpConnect()
	defer conn.Close()
	failOnError(err, "Failed to connect to RabbitMQ")
	ch, err := conn.Channel()
	defer ch.Close()
	failOnError(err, "Failed to open a channel")

	q, err := ch.QueueDeclare(
		"download", // name
		true,
		false,
		false,
		false,
		nil)
	failOnError(err, "Failed to declare a queue")

	reqBodyBytes := new(bytes.Buffer)
	json.NewEncoder(reqBodyBytes).Encode(sprite)

	err = ch.Publish(
		"pokemons", // exchange
		q.Name,     // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(reqBodyBytes.Bytes()),
		})
}

func processMessage(msgs <-chan amqp.Delivery) {
	p := new(Pokemon)
	for d := range msgs {
		json.Unmarshal(d.Body, p)
		download(p)
		d.Ack(true)
	}
}

func consumeSprites() {
	conn, err := amqpConnect()
	defer conn.Close()
	failOnError(err, "Can't connect!")

	ch, err := conn.Channel()
	defer conn.Close()
	failOnError(err, "Can't get connection channel!")

	queue, _ := ch.QueueDeclare(
		"urls_sprites",
		true,
		false,
		false,
		false,
		nil)
	failOnError(err, "Can't declare Queue")

	msgs, err := ch.Consume(
		queue.Name,
		"urls_sprites",
		false, // "ACK"
		false, // "Exclusive"
		false,
		false,
		nil)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	fmt.Printf("[*] Consuming Sprites\n")
	processMessage(msgs)

	<-forever
}

func main() {
	consumeSprites()
}
