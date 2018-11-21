package postgres

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/go-pg/pg"
	"github.com/pkg/errors"

	api "github.com/unchartedsoftware/distil-compute/model"
	"github.com/unchartedsoftware/distil-ingest/conf"
	"github.com/unchartedsoftware/distil-ingest/postgres/model"
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
	DB                  *pg.DB
	Tables              map[string]*model.Dataset
	batchSize           int
	threadCountPG       int
	threadCountGo       int
	batchChannel        chan *batch
	recordChannel       chan *record
	parsedRecordChannel chan *parsedRecord
	errorChannels       []chan error
}

type record struct {
	tableName string
	data      []string
}

type parsedRecord struct {
	tableName string
	insert    string
	args      []interface{}
	err       error
}

type batch struct {
	inserts      *model.InsertBatch
	tableName    string
	errorChannel chan error
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
		DB:                  db,
		Tables:              make(map[string]*model.Dataset),
		batchSize:           config.DBBatchSize,
		threadCountPG:       config.DBThreadCountPG,
		threadCountGo:       config.DBThreadCountGo,
		batchChannel:        make(chan *batch, 10),
		recordChannel:       make(chan *record, 10000),
		parsedRecordChannel: make(chan *parsedRecord, 10000),
	}

	database.Tables[wordStemTableName] = model.NewDataset(wordStemTableName, wordStemTableName, "", nil)

	for i := 0; i < config.DBThreadCountPG; i++ {
		go database.runBatchIngest(database.batchChannel)
	}

	for i := 0; i < config.DBThreadCountGo; i++ {
		go database.runRecordIngest(database.recordChannel, database.parsedRecordChannel)
	}

	go database.runBatchBuilder(database.parsedRecordChannel)

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

func (d *Database) SubmitRecord(tableName string, data []string) {
	record := &record{
		tableName: tableName,
		data:      data,
	}

	d.recordChannel <- record
}

func (d *Database) submitBatch(tableName string) {
	errorChannel := make(chan error, 1)
	d.errorChannels = append(d.errorChannels, errorChannel)

	ds := d.Tables[tableName]
	batch := &batch{
		inserts:      ds.GetBatch(),
		tableName:    tableName,
		errorChannel: errorChannel,
	}

	d.batchChannel <- batch
}

func (d *Database) runBatchIngest(input chan *batch) {
	for batch := range input {
		batch.errorChannel <- d.executeInserts(batch.tableName, batch.inserts)
	}
}

func (d *Database) runBatchBuilder(input chan *parsedRecord) {
	for parsedRecord := range input {
		ds := d.Tables[parsedRecord.tableName]
		ds.AddInsert(parsedRecord.insert, parsedRecord.args)

		if ds.GetBatchSize() >= d.batchSize {
			d.submitBatch(parsedRecord.tableName)
			ds.ResetBatch()
		}
	}
}

func (d *Database) runRecordIngest(input chan *record, output chan *parsedRecord) {
	for record := range input {
		//TODO: Handle potential errors from ingest row call.
		output <- d.IngestRow(record.tableName, record.data)
	}
}

func (d *Database) executeInserts(tableName string, batch *model.InsertBatch) error {
	insertStatement := fmt.Sprintf("INSERT INTO %s.%s.%s_base VALUES %s;", "distil", "public", tableName, strings.Join(batch.GetInsertBatch(), ", "))
	_, err := d.DB.Exec(insertStatement, batch.GetInsertArgs()...)

	return err
}

func (d *Database) executeInsertsComplete(tableName string) error {
	ds := d.Tables[tableName]
	batch := ds.GetBatch()

	_, err := d.DB.Exec(strings.Join(batch.GetInsertBatch(), " "), batch.GetInsertArgs()...)

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
func (d *Database) IngestRow(tableName string, data []string) *parsedRecord {
	ds := d.Tables[tableName]

	insertStatement := ""
	variables := ds.Variables
	values := make([]interface{}, len(variables))

	for i := 0; i < len(variables); i++ {
		// Default columns that have an empty column.
		var val interface{}
		if d.isNullVariable(variables[i].Type, data[i]) {
			val = nil
		} else if d.isArray(variables[i].Type) {
			val = fmt.Sprintf("{%s}", data[i])
		} else {
			val = data[i]
		}
		insertStatement = fmt.Sprintf("%s, ?", insertStatement)
		values[i] = val
	}
	insertStatement = fmt.Sprintf("(%s)", insertStatement[2:])

	return &parsedRecord{
		args:      values,
		insert:    insertStatement,
		tableName: tableName,
	}
}

// InsertRemainingRows empties all batches and inserts the data to the database.
func (d *Database) InsertRemainingRows() error {
	for tableName, ds := range d.Tables {
		if ds.GetBatchSize() > 0 {
			if tableName != wordStemTableName {
				batch := ds.GetBatch()
				err := d.executeInserts(tableName, batch)
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

// Complete run collates all the errors from the batches inserted and
// closes the channels.
func (d *Database) CompleteRun() []error {
	// Collect all the errors and close all the channels.
	errors := make([]error, 0)
	for _, errChannel := range d.errorChannels {
		err := <-errChannel
		if err != nil {
			errors = append(errors, err)
		}
		close(errChannel)
	}

	return errors
}

// AddWordStems builds the word stemming lookup in the database.
func (d *Database) AddWordStems(data []string) error {
	ds := d.Tables[wordStemTableName]

	for _, field := range data {
		// split the field into tokens.
		fields := strings.Fields(field)
		for _, f := range fields {
			fieldValue := wordRegex.ReplaceAllString(f, "")
			if fieldValue == "" {
				continue
			}

			// query for the stemmed version of each word.
			query := fmt.Sprintf("INSERT INTO %s VALUES (unnest(tsvector_to_array(to_tsvector(?))), ?) ON CONFLICT (stem) DO NOTHING;", wordStemTableName)
			ds.AddInsert(query, []interface{}{fieldValue, strings.ToLower(fieldValue)})
			if ds.GetBatchSize() >= d.batchSize {
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
			varsView, variable.Name, d.mapType(variable.Type), d.defaultValue(variable.Type), variable.Name)
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

func (d *Database) mapType(typ string) string {
	// NOTE: current classification has issues so if numerical, assume float64.
	switch typ {
	case "index":
		return "INTEGER"
	case "integer":
		return "FLOAT8"
	case "float", "real":
		return "FLOAT8"
	case "longitude":
		return "FLOAT8"
	case "latitude":
		return "FLOAT8"
	case "realVector":
		return "FLOAT[]"
	default:
		return "TEXT"
	}
}

func (d *Database) defaultValue(typ string) interface{} {
	switch typ {
	case "index":
		return int(0)
	case "integer":
		return float64(0)
	case "float", "real":
		return float64(0)
	case "longitude":
		return float64(0)
	case "latitude":
		return float64(0)
	case "realVector":
		return "'{}'"
	default:
		return "''"
	}
}

func (d *Database) isNullVariable(typ, value string) bool {
	return value == "" && nonNullableTypes[typ]
}

func (d *Database) isArray(typ string) bool {
	return strings.HasSuffix(typ, "Vector")
}
