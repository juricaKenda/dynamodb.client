package query

type SortKeyCondition string

const (
	BeginsWith       SortKeyCondition = "BEGINS_WITH"
	Equals           SortKeyCondition = "EQUALS"
	GreaterThan      SortKeyCondition = "GREATER_THAN"
	GreaterThanEqual SortKeyCondition = "GREATER_THAN_EQUAL"
	LessThan         SortKeyCondition = "LESS_THAN"
	LessThanEqual    SortKeyCondition = "LESS_THAN_EQUAL"
)
