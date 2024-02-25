package query

// SortKeyFilter defines a condition and a value for a sort key.
type SortKeyFilter struct {
	Condition SortKeyCondition
	Value     string
}
