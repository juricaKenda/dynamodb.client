package client

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/juricaKenda/dynamodb.client/pkg/client/consts"
)

type QueryIterator struct {
	paginator *dynamodb.QueryPaginator
}

// Query method
func (c *Client) Query(PK string, skCondition SortKeyCondition, SK string) (*QueryIterator, error) {
	req, err := buildReq(c.table, PK, skCondition, SK)
	if err != nil {
		return nil, err
	}

	iterator := newIterator(dynamodb.NewQueryPaginator(c.dynamo, req))
	return iterator, nil
}

func newIterator(paginator *dynamodb.QueryPaginator) *QueryIterator {
	return &QueryIterator{
		paginator: paginator,
	}
}

func (q *QueryIterator) HasNext() bool {
	return q.paginator.HasMorePages()
}

func (q *QueryIterator) Next(values interface{}) error {
	result, err := q.paginator.NextPage(context.Background())
	if err != nil {
		return err
	}

	if len(result.Items) == 0 {
		if err = json.Unmarshal([]byte("[]"), values); err != nil {
			return err
		}
		return nil
	}

	if err = attributevalue.UnmarshalListOfMaps(result.Items, values); err != nil {
		return nil
	}
	return nil
}

func buildReq(table, PK string, skCondition SortKeyCondition, SK string) (*dynamodb.QueryInput, error) {
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
		TableName: aws.String(table),

		ExpressionAttributeNames:  spec.Names(),
		ExpressionAttributeValues: spec.Values(),
		KeyConditionExpression:    spec.KeyCondition(),
	}, nil
}

type SortKeyCondition string

const (
	BeginsWith SortKeyCondition = "BEGINS_WITH"
)
