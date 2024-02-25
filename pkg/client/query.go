package client

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/juricaKenda/dynamodb.client/pkg/client/consts"
	"github.com/juricaKenda/dynamodb.client/pkg/client/internal/query"
)

// QueryAll returns all items from the table under a given PK + SK filter combination.
func (c *Client) QueryAll(PK string, SK *query.SortKeyFilter, values interface{}) error {
	iter, err := c.Query(PK, SK)
	if err != nil {
		return err
	}

	allItems := make([]map[string]types.AttributeValue, 0)
	for iter.HasNext() {
		items, err := iter.next()
		if err != nil {
			return err
		}

		allItems = append(allItems, items...)
	}

	return attributevalue.UnmarshalListOfMaps(allItems, values)
}

/*
Query the table for results under a given PK + SK filter combination.
Performing this call will not send any subsequent requests to DynamoDB, to perform the requests,
clients need to use the Query iterator and its "HasNext" and "Next" operations.
*/
func (c *Client) Query(PK string, SK *query.SortKeyFilter) (*QueryIterator, error) {
	req, err := buildReq(c.table, PK, SK)
	if err != nil {
		return nil, err
	}

	iterator := newIterator(dynamodb.NewQueryPaginator(c.dynamo, req))
	return iterator, nil
}

// QueryIterator is a helper struct which clients use to iterate over their query results.
type QueryIterator struct {
	paginator *dynamodb.QueryPaginator
}

// HasNext returns true if there are possibly more results to retrieve from DynamoDB. It returns false otherwise.
func (q *QueryIterator) HasNext() bool {
	return q.paginator.HasMorePages()
}

/*
Next performs a request for retrieving the next page of results in DynamoDB.
It accepts a single argument, "values", which is an address for an array of expected results.
Any values returned from DynamoDB will be marshalled into this address.
*/
func (q *QueryIterator) Next(values interface{}) error {
	items, err := q.next()
	if err != nil {
		return err
	}

	if err = attributevalue.UnmarshalListOfMaps(items, values); err != nil {
		return nil
	}
	return nil
}

func (q *QueryIterator) next() ([]map[string]types.AttributeValue, error) {
	result, err := q.paginator.NextPage(context.Background())
	if err != nil {
		return nil, err
	}

	return result.Items, nil
}

func newIterator(paginator *dynamodb.QueryPaginator) *QueryIterator {
	return &QueryIterator{
		paginator: paginator,
	}
}

func buildReq(table, PK string, SK *query.SortKeyFilter) (*dynamodb.QueryInput, error) {
	exp := expression.NewBuilder()
	keyCondition := expression.Key(consts.PK).Equal(expression.Value(PK))
	if SK != nil {
		switch SK.Condition {
		case query.BeginsWith:
			keyCondition = keyCondition.And(expression.Key(consts.SK).BeginsWith(SK.Value))
		case query.Equals:
			keyCondition = keyCondition.And(expression.Key(consts.SK).Equal(expression.Value(SK.Value)))
		case query.GreaterThan:
			keyCondition = keyCondition.And(expression.Key(consts.SK).GreaterThan(expression.Value(SK.Value)))
		case query.GreaterThanEqual:
			keyCondition = keyCondition.And(expression.Key(consts.SK).GreaterThanEqual(expression.Value(SK.Value)))
		case query.LessThan:
			keyCondition = keyCondition.And(expression.Key(consts.SK).LessThan(expression.Value(SK.Value)))
		case query.LessThanEqual:
			keyCondition = keyCondition.And(expression.Key(consts.SK).LessThanEqual(expression.Value(SK.Value)))
		default:
			return nil, errors.New("unimplemented sk condition type")
		}
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
