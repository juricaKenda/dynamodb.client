package client

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/juricaKenda/dynamodb.client/pkg/client/consts"
)

/*
Set item under a given PK + SK combination. If an item already exists, it will be overwritten with new values.
The `input` must have json tags in order to be properly processed and stored.
It also MUST NOT contain any of the following tags {"PK", "SK"} as those are reserved by the library.
*/
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

/*
Get item stored under a given PK + SK combination. An `input` is an address of a container in which result will be stored.
If there is no item to return, the input will remain unmodified and no errors will be returned.
*/
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

/*
Del item under a given PK + SK combination.
If there are no items to delete, the call will still return no errors.
*/
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
