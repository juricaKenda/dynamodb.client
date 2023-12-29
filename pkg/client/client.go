package client

import (
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Client struct {
	dynamo *dynamodb.DynamoDB
	table  string
}

// New client constructor
func New(table, region, endpoint string) (*Client, error) {
	session, err := session.NewSessionWithOptions(
		session.Options{
			Config: aws.Config{
				Endpoint: aws.String(endpoint),
				Region:   aws.String(region),
			},
		})
	if err != nil {
		return nil, err
	}
	client := dynamodb.New(session)

	return &Client{
		table:  table,
		dynamo: client,
	}, pingTable(client, table)
}

func pingTable(db *dynamodb.DynamoDB, tableName string) error {
	tables, err := db.ListTables(&dynamodb.ListTablesInput{})
	if err != nil {
		return err
	}

	for _, name := range tables.TableNames {
		if tableName == *name {
			return nil
		}
	}

	return errors.New("table not found in DynamoDB")
}
