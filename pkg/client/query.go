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

type QueryHelper struct {
	client *Client
}

// Query method
func (c *Client) Query(values interface{}, PK string, skCondition SortKeyCondition, SK string) error {
	helper := newHelper(c)
	req, err := helper.buildReq(PK, skCondition, SK)
	if err != nil {
		return err
	}

	return helper.depaginate(values, req)
}

func newHelper(client *Client) *QueryHelper {
	return &QueryHelper{
		client: client,
	}
}

func (q *QueryHelper) depaginate(values interface{}, req *dynamodb.QueryInput) error {
	lastEvaluated, err := q.nextQuery(values, req)
	if err != nil {
		return err
	}

	for lastEvaluated != nil {
		req.ExclusiveStartKey = lastEvaluated
		lastEvaluated, err = q.nextQuery(values, req)
		if err != nil {
			return err
		}
	}
	return nil
}

func (q *QueryHelper) nextQuery(values interface{}, req *dynamodb.QueryInput) (map[string]*dynamodb.AttributeValue, error) {
	if values == nil {
		return nil, errors.New("given pointer to output values was nil")
	}

	result, err := q.client.dynamo.Query(req)
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

func (q *QueryHelper) buildReq(PK string, skCondition SortKeyCondition, SK string) (*dynamodb.QueryInput, error) {
	exp := expression.NewBuilder()
	keyCondition := expression.Key(consts.PK).Equal(expression.Value(PK))
	switch skCondition {
	case BeginsWith:
		keyCondition = keyCondition.And(expression.Key(consts.SK).BeginsWith(SK))
	default:
		return nil, errors.New("unimplemented sk condition type")
	}

	exp = exp.WithKeyCondition(keyCondition)
	spec, err := exp.Build()
	if err != nil {
		return nil, errors.New("building expression")
	}

	return &dynamodb.QueryInput{
		TableName: aws.String(q.client.table),

		ExpressionAttributeNames:  spec.Names(),
		ExpressionAttributeValues: spec.Values(),
		KeyConditionExpression:    spec.KeyCondition(),
	}, nil
}

type SortKeyCondition string

const (
	BeginsWith SortKeyCondition = "BEGINS_WITH"
)
