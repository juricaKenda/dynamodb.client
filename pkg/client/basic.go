package client

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/juricaKenda/dynamodb.client/pkg/client/consts"
)

// Set method
func (c *Client) Set(PK, SK string, input interface{}) error {
	dynamoModel, err := attributevalue.MarshalMap(input)
	if err != nil {
		return err
	}

	for key, val := range keys(PK, SK) {
		dynamoModel[key] = val
	}

	request := &dynamodb.PutItemInput{
		TableName: aws.String(c.table),
		Item:      dynamoModel,
	}

	_, err = c.dynamo.PutItem(context.Background(), request)
	return err
}

// Get method
func (c *Client) Get(PK, SK string, input interface{}) error {
	req := &dynamodb.GetItemInput{
		TableName: aws.String(c.table),
		Key:       keys(PK, SK),
	}

	result, err := c.dynamo.GetItem(context.Background(), req)
	if err != nil {
		return err
	}

	if result.Item == nil {
		return nil
	}

	return attributevalue.UnmarshalMap(result.Item, input)
}

// Del method
func (c *Client) Del(PK, SK string) error {
	request := &dynamodb.DeleteItemInput{
		TableName: aws.String(c.table),
		Key:       keys(PK, SK),
	}

	_, err := c.dynamo.DeleteItem(context.Background(), request)
	return err
}

func keys(PK string, SK string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		consts.PK: &types.AttributeValueMemberS{Value: PK},
		consts.SK: &types.AttributeValueMemberS{Value: SK},
	}
}
