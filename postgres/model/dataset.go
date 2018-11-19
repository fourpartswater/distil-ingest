package model

import (
	"github.com/unchartedsoftware/distil-compute/model"
)

// Dataset is a struct containing the metadata of a dataset being processed.
type Dataset struct {
	ID              string
	Name            string
	Description     string
	Variables       []*model.Variable
	variablesLookup map[string]bool
	insertBatch     *InsertBatch
}

// InsertBatch is a struct for the batch data.
type InsertBatch struct {
	insertBatch []string
	insertArgs  []interface{}
}

// NewDataset creates a new dataset instance.
func NewDataset(id, name, description string, meta *model.Metadata) *Dataset {
	ds := &Dataset{
		ID:              id,
		Name:            name,
		Description:     description,
		variablesLookup: make(map[string]bool),
		insertBatch:     newBatch(),
	}
	// NOTE: Can only support data in a single data resource for now.
	if meta != nil {
		ds.Variables = meta.DataResources[0].Variables
	}

	return ds
}

func newBatch() *InsertBatch {
	return &InsertBatch{
		insertBatch: make([]string, 0),
		insertArgs:  make([]interface{}, 0),
	}
}

// AddInsert adds an insert to the batch.
func (b *InsertBatch) AddInsert(statement string, args []interface{}) {
	b.insertBatch = append(b.insertBatch, statement)
	b.insertArgs = append(b.insertArgs, args...)
}

// ResetBatch clears the batch contents.
func (ds *Dataset) ResetBatch() {
	ds.insertBatch = newBatch()
}

// HasVariable checks to see if a variable is already contained in the dataset.
func (ds *Dataset) HasVariable(variable *model.Variable) bool {
	return ds.variablesLookup[variable.Name]
}

// AddVariable adds a variable to the dataset.
func (ds *Dataset) AddVariable(variable *model.Variable) {
	ds.Variables = append(ds.Variables, variable)
	ds.variablesLookup[variable.Name] = true
}

// AddInsert adds an insert statement and parameters to the batch.
func (ds *Dataset) AddInsert(statement string, args []interface{}) {
	ds.insertBatch.AddInsert(statement, args)
}

// GetBatchSize gets the insert batch count.
func (ds *Dataset) GetBatchSize() int {
	return len(ds.insertBatch.insertBatch)
}

// GetInsertBatch returns the batched inserts.
func (ds *Dataset) GetBatch() *InsertBatch {
	return ds.insertBatch
}

// GetInsertBatch gets the inserts added to the batch.
func (i *InsertBatch) GetInsertBatch() []string {
	return i.insertBatch
}

// GetInsertArgs returns the arguments mapped to the inserts in the batch.
func (i *InsertBatch) GetInsertArgs() []interface{} {
	return i.insertArgs
}
