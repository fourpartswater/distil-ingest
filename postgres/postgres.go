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

package postgres

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/go-pg/pg"
	"github.com/pkg/errors"

	api "github.com/uncharted-distil/distil-compute/model"
	"github.com/uncharted-distil/distil-ingest/conf"
	"github.com/uncharted-distil/distil-ingest/postgres/model"
	"github.com/unchartedsoftware/deluge/document"
	"github.com/unchartedsoftware/plog"
)

const (
	metadataTableCreationSQL = `CREATE TABLE %s (
			name	varchar(100)	NOT NULL,
			role	varchar(100),
			type	varchar(100)
		);`
	resultTableCreationSQL = `CREATE TABLE %s (
			result_id	varchar(1000)	NOT NULL,
			index		BIGINT,
			target		varchar(100),
			value		varchar(200)
		);`

	requestTableName        = "request"
	solutionTableName       = "solution"
	solutionResultTableName = "solution_result"
	solutionScoreTableName  = "solution_score"
	requestFeatureTableName = "request_feature"
	requestFilterTableName  = "request_filter"
	wordStemTableName       = "word_stem"

	requestTableCreationSQL = `CREATE TABLE %s (
			request_id			varchar(200),
			dataset				varchar(200),
			progress			varchar(40),
			created_time		timestamp,
			last_updated_time	timestamp
		);`
	solutionTableCreationSQL = `CREATE TABLE %s (
			request_id		varchar(200),
			solution_id		varchar(200),
			progress		varchar(40),
			created_time	timestamp,
			deleted         boolean
		);`
	requestFeatureTableCreationSQL = `CREATE TABLE %s (
			request_id		varchar(200),
			feature_name	varchar(100),
			feature_type	varchar(20)
		);`
	requestFilterTableCreationSQL = `CREATE TABLE %s (
			request_id			varchar(200),
			feature_name		varchar(100),
			filter_type			varchar(40),
			filter_mode			varchar(40),
			filter_min			double precision,
			filter_max			double precision,
			filter_min_x		double precision,
			filter_max_x		double precision,
			filter_min_y		double precision,
			filter_max_y		double precision,
			filter_categories	varchar(200),
			filter_indices		varchar(200)
		);`
	solutionScoreTableCreationSQL = `CREATE TABLE %s (
			solution_id	varchar(200),
			metric		varchar(40),
			score		double precision
		);`
	solutionResultTableCreationSQL = `CREATE TABLE %s (
			solution_id			varchar(200),
			fitted_solution_id	varchar(200),
			result_uuid			varchar(200),
			result_uri			varchar(200),
			progress			varchar(40),
			created_time		timestamp
		);`
	wordStemsTableCreationSQL = `CREATE TABLE %s (
			stem		varchar(200) PRIMARY KEY,
			word		varchar(200)
		);`

	resultTableSuffix   = "_result"
	variableTableSuffix = "_variable"
)

var (
	nonNullableTypes = map[string]bool{
		"index":   true,
		"integer": true,
		"float":   true,
		"real":    true,
	}
	wordRegex = regexp.MustCompile("[^a-zA-Z]")
)

// Database is a struct representing a full logical database.
type Database struct {
	DB        *pg.DB
	Tables    map[string]*model.Dataset
	BatchSize int
}

// WordStem contains the pairing of a word and its stemmed version.
type WordStem struct {
	Word string
	Stem string
}

// NewDatabase creates a new database instance.
func NewDatabase(config *conf.Conf) (*Database, error) {
	db := pg.Connect(&pg.Options{
		Addr:     fmt.Sprintf("%s:%d", config.DBHost, config.DBPort),
		User:     config.DBUser,
		Password: config.DBPassword,
		Database: config.Database,
	})

	database := &Database{
		DB:        db,
		Tables:    make(map[string]*model.Dataset),
		BatchSize: config.DBBatchSize,
	}

	database.Tables[wordStemTableName] = model.NewDataset(wordStemTableName, wordStemTableName, "", nil)

	return database, nil
}

// CreateSolutionMetadataTables creates an empty table for the solution results.
func (d *Database) CreateSolutionMetadataTables() error {
	// Create the solution tables.
	log.Infof("Creating solution metadata tables.")

	d.DropTable(requestTableName)
	_, err := d.DB.Exec(fmt.Sprintf(requestTableCreationSQL, requestTableName))
	if err != nil {
		return err
	}

	d.DropTable(requestFeatureTableName)
	_, err = d.DB.Exec(fmt.Sprintf(requestFeatureTableCreationSQL, requestFeatureTableName))
	if err != nil {
		return err
	}

	d.DropTable(requestFilterTableName)
	_, err = d.DB.Exec(fmt.Sprintf(requestFilterTableCreationSQL, requestFilterTableName))
	if err != nil {
		return err
	}

	d.DropTable(solutionTableName)
	_, err = d.DB.Exec(fmt.Sprintf(solutionTableCreationSQL, solutionTableName))
	if err != nil {
		return err
	}

	d.DropTable(solutionResultTableName)
	_, err = d.DB.Exec(fmt.Sprintf(solutionResultTableCreationSQL, solutionResultTableName))
	if err != nil {
		return err
	}

	d.DropTable(solutionScoreTableName)
	_, err = d.DB.Exec(fmt.Sprintf(solutionScoreTableCreationSQL, solutionScoreTableName))
	if err != nil {
		return err
	}

	// do not drop the word stem table as we want it to include all words.
	_, err = d.DB.Exec(fmt.Sprintf(wordStemsTableCreationSQL, wordStemTableName))
	// ignore the error in the word stem creation.
	// Almost certainly due to the table already existing.

	return nil
}

func (d *Database) executeInserts(tableName string) error {
	ds := d.Tables[tableName]

	insertStatement := fmt.Sprintf("INSERT INTO %s.%s.%s_base VALUES %s;", "distil", "public", tableName, strings.Join(ds.GetBatch(), ", "))

	_, err := d.DB.Exec(insertStatement, ds.GetBatchArgs()...)

	return err
}

func (d *Database) executeInsertsComplete(tableName string) error {
	ds := d.Tables[tableName]

	_, err := d.DB.Exec(strings.Join(ds.GetBatch(), " "), ds.GetBatchArgs()...)

	return err
}

// CreateResultTable creates an empty table for the solution results.
func (d *Database) CreateResultTable(tableName string) error {
	resultTableName := fmt.Sprintf("%s%s", tableName, resultTableSuffix)

	// Make sure the table is clear. If the table did not previously exist,
	// an error is returned. May as well ignore it since a serious problem
	// will cause errors on the other statements as well.
	err := d.DropTable(resultTableName)

	// Create the variable table.
	log.Infof("Creating result table %s", resultTableName)
	createStatement := fmt.Sprintf(resultTableCreationSQL, resultTableName)
	_, err = d.DB.Exec(createStatement)
	if err != nil {
		return err
	}

	return nil
}

// StoreMetadata stores the variable information to the specified table.
func (d *Database) StoreMetadata(tableName string) error {
	variableTableName := fmt.Sprintf("%s%s", tableName, variableTableSuffix)

	// Make sure the table is clear. If the table did not previously exist,
	// an error is returned. May as well ignore it since a serious problem
	// will cause errors on the other statements as well.
	err := d.DropTable(variableTableName)

	// Create the variable table.
	log.Infof("Creating variable table %s", variableTableName)
	createStatement := fmt.Sprintf(metadataTableCreationSQL, variableTableName)
	_, err = d.DB.Exec(createStatement)
	if err != nil {
		return err
	}

	// Insert the variable metadata into the new table.
	for _, v := range d.Tables[tableName].Variables {
		insertStatement := fmt.Sprintf("INSERT INTO %s (name, role, type) VALUES (?, ?, ?);", variableTableName)
		values := []interface{}{v.Name, v.Role, v.Type}
		_, err = d.DB.Exec(insertStatement, values...)
		if err != nil {
			return err
		}
	}

	return nil
}

// DeleteDataset deletes all tables & views for a dataset.
func (d *Database) DeleteDataset(name string) {
	baseName := fmt.Sprintf("%s_base", name)
	resultName := fmt.Sprintf("%s%s", name, resultTableSuffix)
	variableName := fmt.Sprintf("%s%s", name, variableTableSuffix)

	d.DropView(name)
	d.DropTable(baseName)
	d.DropTable(resultName)
	d.DropTable(variableName)
}

// IngestRow parses the raw csv data and stores it to the table specified.
// The previously parsed metadata is used to map columns.
func (d *Database) IngestRow(tableName string, data string) error {
	ds := d.Tables[tableName]

	insertStatement := ""
	variables := ds.Variables
	values := make([]interface{}, len(variables))
	doc := &document.CSV{}
	doc.SetData(data)

	// If a row ends in a delimeter, deluge does not add the last field.
	if len(doc.Cols) == len(variables)-1 && strings.HasSuffix(data, ",") {
		doc.Cols = append(doc.Cols, "")
	}
	for i := 0; i < len(variables); i++ {
		// Default columns that have an empty column.
		var val interface{}
		if d.isNullVariable(variables[i].Type, doc.Cols[i]) {
			val = nil
		} else if d.isArray(variables[i].Type) {
			val = fmt.Sprintf("{%s}", doc.Cols[i])
		} else {
			val = doc.Cols[i]
		}
		insertStatement = fmt.Sprintf("%s, ?", insertStatement)
		values[i] = val
	}
	insertStatement = fmt.Sprintf("(%s)", insertStatement[2:])
	ds.AddInsert(insertStatement, values)

	if ds.GetBatchSize() >= d.BatchSize {
		err := d.executeInserts(tableName)
		if err != nil {
			return errors.Wrap(err, "unable to insert to table "+tableName)
		}

		ds.ResetBatch()
	}

	return nil
}

// InsertRemainingRows empties all batches and inserts the data to the database.
func (d *Database) InsertRemainingRows() error {
	for tableName, ds := range d.Tables {
		if ds.GetBatchSize() > 0 {
			if tableName != wordStemTableName {
				err := d.executeInserts(tableName)
				if err != nil {
					return errors.Wrap(err, "unable to insert remaining rows for table "+tableName)
				}
			} else {
				err := d.executeInsertsComplete(tableName)
				if err != nil {
					return errors.Wrap(err, "unable to insert remaining rows for table "+tableName)
				}
			}
		}
	}

	return nil
}

// AddWordStems builds the word stemming lookup in the database.
func (d *Database) AddWordStems(data string) error {
	ds := d.Tables[wordStemTableName]

	// process every field (assume csv).
	doc := &document.CSV{}
	doc.SetData(data)

	for i := 0; i < len(doc.Cols); i++ {
		// split the field into tokens.
		fields := strings.Fields(doc.Cols[i])
		for _, f := range fields {
			fieldValue := wordRegex.ReplaceAllString(f, "")
			if fieldValue == "" {
				continue
			}

			// query for the stemmed version of each word.
			query := fmt.Sprintf("INSERT INTO %s VALUES (unnest(tsvector_to_array(to_tsvector(?))), ?) ON CONFLICT (stem) DO NOTHING;", wordStemTableName)
			ds.AddInsert(query, []interface{}{fieldValue, strings.ToLower(fieldValue)})
			if ds.GetBatchSize() >= d.BatchSize {
				err := d.executeInsertsComplete(wordStemTableName)
				if err != nil {
					return errors.Wrap(err, "unable to insert to table "+wordStemTableName)
				}

				ds.ResetBatch()
			}
		}
	}

	return nil
}

// DropTable drops the specified table from the database.
func (d *Database) DropTable(tableName string) error {
	log.Infof("Dropping table %s", tableName)
	drop := fmt.Sprintf("DROP TABLE %s;", tableName)
	_, err := d.DB.Exec(drop)
	log.Infof("Dropped table %s", tableName)

	return err
}

// DropView drops the specified view from the database.
func (d *Database) DropView(viewName string) error {
	log.Infof("Dropping view %s", viewName)
	drop := fmt.Sprintf("DROP VIEW %s;", viewName)
	_, err := d.DB.Exec(drop)
	log.Infof("Dropped view %s", viewName)

	return err
}

// InitializeTable generates and runs a table create statement based on the schema.
func (d *Database) InitializeTable(tableName string, ds *model.Dataset) error {
	d.Tables[tableName] = ds

	// Create the view and table statements.
	// The table has everything stored as a string.
	// The view uses casting to set the types.
	createStatementTable := `CREATE TABLE %s_base (%s);`
	createStatementView := `CREATE VIEW %s AS SELECT %s FROM %s_base;`
	varsTable := ""
	varsView := ""
	for _, variable := range ds.Variables {
		varsTable = fmt.Sprintf("%s\n\"%s\" TEXT,", varsTable, variable.Name)
		varsView = fmt.Sprintf("%s\nCOALESCE(CAST(\"%s\" AS %s), %v) AS \"%s\",",
			varsView, variable.Name, api.MapD3MTypeToPostgresType(variable.Type), api.DefaultPostgresValueFromD3MType(variable.Type), variable.Name)
	}
	if len(varsTable) > 0 {
		varsTable = varsTable[:len(varsTable)-1]
		varsView = varsView[:len(varsView)-1]
	}
	createStatementTable = fmt.Sprintf(createStatementTable, tableName, varsTable)
	log.Infof("Creating table %s_base", tableName)

	// Create the table.
	_, err := d.DB.Exec(createStatementTable)
	if err != nil {
		return err
	}

	createStatementView = fmt.Sprintf(createStatementView, tableName, varsView, tableName)
	log.Infof("Creating view %s", tableName)

	// Create the table.
	_, err = d.DB.Exec(createStatementView)
	if err != nil {
		return err
	}

	return nil
}

// InitializeDataset initializes the dataset with the provided metadata.
func (d *Database) InitializeDataset(meta *api.Metadata) (*model.Dataset, error) {
	ds := model.NewDataset(meta.ID, meta.Name, meta.Description, meta)

	return ds, nil
}

func (d *Database) isNullVariable(typ, value string) bool {
	return value == "" && nonNullableTypes[typ]
}

func (d *Database) isArray(typ string) bool {
	return strings.HasSuffix(typ, "Vector")
}
