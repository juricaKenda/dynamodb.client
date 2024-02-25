package examples

import (
	"github.com/juricaKenda/dynamodb.client/pkg/client"
	"time"
)

func main() {
	db, _ := client.New("table-name", "region", "endpoint", nil)

	// simple put example
	_ = db.Put("my-partition", "my-sort", Value{A: "a", B: 123, C: true})

	// put with options
	expireAt := time.Now().Add(time.Hour)
	_ = db.Put("my-partition", "my-sort", Value{A: "a", B: 213, C: false}, client.TTL(expireAt.Unix()))
}

type Value struct {
	A string `dynamodbav:"A"`
	B int64  `dynamodbav:"B"`
	C bool   `dynamodbav:"C"`
}
