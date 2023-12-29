package client

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/juricaKenda/dynamodb.client/pkg/client/consts"
)

// Set method
func (c *Client) Set(PK, SK string, input interface{}) error {
	dynamoModel, err := dynamodbattribute.MarshalMap(input)
	if err != nil {
		return err
	}

	dynamoModel[consts.PK] = &dynamodb.AttributeValue{S: aws.String(PK)}
	dynamoModel[consts.SK] = &dynamodb.AttributeValue{S: aws.String(SK)}

	request := &dynamodb.PutItemInput{
		TableName: aws.String(c.table),
		Item:      dynamoModel,
	}

	_, err = c.dynamo.PutItem(request)
	return err
}

// Get method
func (c *Client) Get(PK, SK string, input interface{}) error {
	req := &dynamodb.GetItemInput{
		TableName: aws.String(c.table),
		Key: map[string]*dynamodb.AttributeValue{
			consts.PK: {S: aws.String(PK)},
			consts.SK: {S: aws.String(SK)},
		},
	}

	result, err := c.dynamo.GetItem(req)
	if err != nil {
		return err
	}

	if result.Item == nil {
		return nil
	}

	return dynamodbattribute.UnmarshalMap(result.Item, input)
}

// Del method
func (c *Client) Del(PK, SK string) error {
	request := &dynamodb.DeleteItemInput{
		TableName: aws.String(c.table),
		Key: map[string]*dynamodb.AttributeValue{
			consts.PK: {S: aws.String(PK)},
			consts.SK: {S: aws.String(SK)},
		},
	}

	_, err := c.dynamo.DeleteItem(request)
	return err
}
