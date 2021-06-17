package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/uutarou10/remo-sensor-value-logger/handler"
)

func main() {
	lambda.Start(handler.Handler)
}
