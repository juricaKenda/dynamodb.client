package client

import (
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/juricaKenda/dynamodb.client/pkg/client/consts"
)

type QueryIterator struct {
	client   *Client
	query    *dynamodb.QueryInput
	firstRun bool
}

// Query method
func (c *Client) Query(PK string, skCondition SortKeyCondition, SK string) (*QueryIterator, error) {
	iterator, err := newIterator(c, PK, skCondition, SK)
	if err != nil {
		return nil, err
	}

	return iterator, nil
}

func newIterator(client *Client, PK string, skCondition SortKeyCondition, SK string) (*QueryIterator, error) {
	q := &QueryIterator{client: client}
	return q, q.buildReq(PK, skCondition, SK)
}

func (q *QueryIterator) HasNext() bool {
	return q.firstRun || q.query.ExclusiveStartKey == nil
}

func (q *QueryIterator) Next(values interface{}) error {
	q.firstRun = false
	lastEvaluated, err := q.execQuery(values)
	if err != nil {
		return err
	}

	q.query.ExclusiveStartKey = lastEvaluated
	return nil
}

func (q *QueryIterator) execQuery(values interface{}) (map[string]*dynamodb.AttributeValue, error) {
	if values == nil {
		return nil, errors.New("given pointer to output values was nil")
	}

	result, err := q.client.dynamo.Query(q.query)
	if err != nil {
		return nil, errors.New("failed to query Dynamo")
	}

	if len(result.Items) == 0 {
		if err = json.Unmarshal([]byte("[]"), values); err != nil {
			return nil, err
		}
		return nil, nil
	}

	if err = dynamodbattribute.UnmarshalListOfMaps(result.Items, values); err != nil {
		return nil, err
	}

	return result.LastEvaluatedKey, nil
}

func (q *QueryIterator) buildReq(PK string, skCondition SortKeyCondition, SK string) error {
	exp := expression.NewBuilder()
	keyCondition := expression.Key(consts.PK).Equal(expression.Value(PK))
	switch skCondition {
	case BeginsWith:
		keyCondition = keyCondition.And(expression.Key(consts.SK).BeginsWith(SK))
	default:
		return errors.New("unimplemented sk condition type")
	}

	exp = exp.WithKeyCondition(keyCondition)
	spec, err := exp.Build()
	if err != nil {
		return errors.New("building expression")
	}

	q.query = &dynamodb.QueryInput{
		TableName: aws.String(q.client.table),

		ExpressionAttributeNames:  spec.Names(),
		ExpressionAttributeValues: spec.Values(),
		KeyConditionExpression:    spec.KeyCondition(),
	}
	return nil
}

type SortKeyCondition string

const (
	BeginsWith SortKeyCondition = "BEGINS_WITH"
)
