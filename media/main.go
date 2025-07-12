package main

import (
	"fmt"
	"os"
)

func main() {
	rabitMqHost := os.Getenv("RABBITMQ_URL")
	rabitMqPort := os.Getenv("RABBITMQ_PORT")
	rabitMqUser := os.Getenv("RABBITMQ_USER")
	rabitMqPass := os.Getenv("RABBITMQ_PASS")

	fmt.Println(rabitMqHost)
	fmt.Println(rabitMqPort)
	fmt.Println(rabitMqUser)
	fmt.Println(rabitMqPass)
	fmt.Println("hello world")
}
