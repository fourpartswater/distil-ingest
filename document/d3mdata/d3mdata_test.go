//
//   Copyright © 2019 Uncharted Software Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package d3mdata

import (
	"testing"

	"github.com/jeffail/gabs"
	"github.com/stretchr/testify/assert"

	"github.com/uncharted-distil/distil-ingest/metadata"
)

func TestGetSource(t *testing.T) {

	meta, err := metadata.LoadMetadataFromOriginalSchema("./testdata/datasetDoc.json")
	assert.NoError(t, err)

	// Create a document using the test json schema
	docCreate, err := NewD3MData(meta)
	assert.NoError(t, err)

	doc, err := docCreate()
	assert.NoError(t, err)

	data := "0,cat_1,99.0,66,ord_1,234324,some text value,target_1"
	doc.SetData(data)

	// Fetch the doc source
	output, err := doc.GetSource()
	assert.NoError(t, err)

	// Extract it from JSON
	result, err := gabs.Consume(output)
	assert.NoError(t, err)

	assert.Equal(t, "cat_1", result.Path("Alpha.value").Data().(string))
	assert.Equal(t, "categorical", result.Path("Alpha.schemaType").Data().(string))

	assert.Equal(t, 99.0, result.Path("Bravo.value").Data().(float64))
	assert.Equal(t, "float", result.Path("Bravo.schemaType").Data().(string))

	assert.Equal(t, int64(66), result.Path("Charlie.value").Data().(int64))
	assert.Equal(t, "integer", result.Path("Charlie.schemaType").Data().(string))

	assert.Equal(t, "ord_1", result.Path("Delta.value").Data().(string))
	assert.Equal(t, "ordinal", result.Path("Delta.schemaType").Data().(string))

	assert.Equal(t, int64(234324), result.Path("Echo.value").Data().(int64))
	assert.Equal(t, "dateTime", result.Path("Echo.schemaType").Data().(string))

	assert.Equal(t, "some text value", result.Path("Foxtrot.value").Data().(string))
	assert.Equal(t, "text", result.Path("Foxtrot.schemaType").Data().(string))

	assert.Equal(t, "target_1", result.Path("Whiskey.value").Data().(string))
	assert.Equal(t, "categorical", result.Path("Whiskey.schemaType").Data().(string))
}

func TestGetMapping(t *testing.T) {

	meta, err := metadata.LoadMetadataFromOriginalSchema("./testdata/datasetDoc.json")
	assert.NoError(t, err)

	// Create a document using the test json schema
	docCreate, err := NewD3MData(meta)
	assert.NoError(t, err)

	doc, err := docCreate()
	assert.NoError(t, err)

	// Fetch the mappings
	strMapping, err := doc.GetMapping()
	assert.NoError(t, err)

	mapping, err := gabs.ParseJSON([]byte(strMapping))
	assert.Equal(t, "keyword", mapping.Path("datum.properties.Alpha.properties.value.type").Data().(string))
	assert.Equal(t, "keyword", mapping.Path("datum.properties.Alpha.properties.schemaType.type").Data().(string))
	assert.Equal(t, "double", mapping.Path("datum.properties.Bravo.properties.value.type").Data().(string))
	assert.Equal(t, "keyword", mapping.Path("datum.properties.Bravo.properties.schemaType.type").Data().(string))
	assert.Equal(t, "long", mapping.Path("datum.properties.Charlie.properties.value.type").Data().(string))
	assert.Equal(t, "keyword", mapping.Path("datum.properties.Charlie.properties.schemaType.type").Data().(string))
	assert.Equal(t, "keyword", mapping.Path("datum.properties.Delta.properties.value.type").Data().(string))
	assert.Equal(t, "keyword", mapping.Path("datum.properties.Delta.properties.schemaType.type").Data().(string))
	assert.Equal(t, "date", mapping.Path("datum.properties.Echo.properties.value.type").Data().(string))
	assert.Equal(t, "keyword", mapping.Path("datum.properties.Echo.properties.schemaType.type").Data().(string))
	assert.Equal(t, "text", mapping.Path("datum.properties.Foxtrot.properties.value.type").Data().(string))
	assert.Equal(t, "keyword", mapping.Path("datum.properties.Foxtrot.properties.schemaType.type").Data().(string))
	assert.Equal(t, "keyword", mapping.Path("datum.properties.Whiskey.properties.value.type").Data().(string))
	assert.Equal(t, "keyword", mapping.Path("datum.properties.Whiskey.properties.schemaType.type").Data().(string))
}

func TestID(t *testing.T) {

	meta, err := metadata.LoadMetadataFromOriginalSchema("./testdata/datasetDoc.json")
	assert.NoError(t, err)

	// Create a document using the test json schema
	docCreate, err := NewD3MData(meta)
	assert.NoError(t, err)

	doc, err := docCreate()
	assert.NoError(t, err)

	data := "0,cat_1,99.0,66,ord_1,234324,some text value"
	doc.SetData(data)

	// Fetch id
	id, err := doc.GetID()
	if err != nil {
		assert.Fail(t, "Failed to create document")
	}

	// Verify the id
	assert.Equal(t, "0", id)
}
