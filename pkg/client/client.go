package client

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type Client struct {
	dynamo *dynamodb.Client
	table  string
}

// New client constructor
func New(table, region, endpoint string, auth *Auth) (*Client, error) {
	client := dynamodb.New(dynamodb.Options{
		BaseEndpoint: aws.String(endpoint),
		Region:       region,
		Credentials:  auth,
	})

	return &Client{
		table:  table,
		dynamo: client,
	}, pingTable(client, table)
}

func pingTable(db *dynamodb.Client, expectedTable string) error {
	tables, err := db.ListTables(context.Background(), &dynamodb.ListTablesInput{})
	if err != nil {
		return err
	}

	for _, name := range tables.TableNames {
		if name == expectedTable {
			return nil
		}
	}

	return errors.New("table not found in DynamoDB")
}

type Auth struct {
	// AWS Access key ID
	AccessKeyID string
	// AWS Secret Access Key
	SecretAccessKey string
}

func (a *Auth) Retrieve(_ context.Context) (aws.Credentials, error) {
	return aws.Credentials{
		AccessKeyID:     a.AccessKeyID,
		SecretAccessKey: a.SecretAccessKey,
	}, nil
}
