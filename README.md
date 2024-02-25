# AWS DynamoDB client wrapper

This is a simple wrapper for the AWS DynamoDB client for Go. It provides a simple way to interact with DynamoDB, without the unnecessary complexity of the AWS SDK.

Recommended usage is for projects leveraging single table design, as some assumptions are made within the wrapper, to further simplify the usage.

Note: _It is a work in progress, and is not yet feature complete and 1:1 with the AWS SDK. If you need a feature that is not yet implemented, feel free to open an issue or a pull request._


## Usage

```go
package example

import (
	"github.com/juricaKenda/dynamodb.client/pkg/client"
	"github.com/juricaKenda/dynamodb.client/pkg/client/internal/query"
)

func main() {
	db, err := client.New("table-name", "region", "endpoint", nil)
	if err != nil {
		panic(err)
	}

	// simple put example
	_ = db.Put("my-partition", "my-sort", Value{A: "a", B: 123, C: true})

	// simple get example
	result := Value{}
	_ = db.Get("my-partition", "my-sort", &result)

	// simple delete example
	_ = db.Del("my-partition", "my-sort")

	// simple query example
	results := []Value{}
	_ = db.QueryAll("my-partition", &query.SortKeyFilter{Condition: query.GreaterThan, Value: "abc" }, &results)
}

type Value struct {
	A string `dynamodbav:"A"`
	B int64  `dynamodbav:"B"`
	C bool   `dynamodbav:"C"`
}

```