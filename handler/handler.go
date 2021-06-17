package handler

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/ssm"
	"github.com/uutarou10/remo-go"
	"strconv"
	"time"
)

func Handler() error {
	awsSession, err := session.NewSession(aws.NewConfig().WithRegion("ap-northeast-1"))
	if err != nil {
		return err
	}

	svc := ssm.New(awsSession)
	paramRes, err := svc.GetParameter(&ssm.GetParameterInput{
		Name:           aws.String("remo-api-token"),
		WithDecryption: aws.Bool(true),
	})
	if err != nil {
		return err
	}

	remoClient := remo.New(*paramRes.Parameter.Value)
	devices, err := remoClient.GetDevices()
	if err != nil {
		return err
	} else if len(devices) <= 0 {
		return fmt.Errorf("device is empty")
	}

	db := dynamodb.New(awsSession)

	ts := aws.String(strconv.FormatInt(time.Now().Unix(), 10))
	writeRequests := []*dynamodb.WriteRequest{
		{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					"SensorType": {S: aws.String("temperature")},
					"Timestamp":  {N: ts},
					"Value":      {N: sensorValueToAwsString(devices[0].NewestEvents.Temperature.Value)},
				},
			},
		},
		{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					"SensorType": {S: aws.String("humidity")},
					"Timestamp":  {N: ts},
					"Value":      {N: sensorValueToAwsString(devices[0].NewestEvents.Humidity.Value)},
				},
			},
		},
		{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					"SensorType": {S: aws.String("illumination")},
					"Timestamp":  {N: ts},
					"Value":      {N: sensorValueToAwsString(devices[0].NewestEvents.Illumination.Value)},
				},
			},
		},
		// movementだけ値の返り方が特殊なのでなんか考えた方がいいかもしれない
		//{
		//	PutRequest: &dynamodb.PutRequest{
		//		Item: map[string]*dynamodb.AttributeValue{
		//			"SensorType": {S: aws.String("movement")},
		//			"Timestamp": {N: ts},
		//			"Value": {N: aws.String("4")},
		//		},
		//	},
		//},
	}

	if result, err := db.BatchWriteItem(&dynamodb.BatchWriteItemInput{RequestItems: map[string][]*dynamodb.WriteRequest{
		"SensorLog": writeRequests,
	}}); err != nil {
		return err
	} else {
		fmt.Printf("Result: %+v", result)
	}

	return nil
}

func sensorValueToAwsString(value float32) *string {
	str := fmt.Sprintf("%g", value)
	return &str
}
