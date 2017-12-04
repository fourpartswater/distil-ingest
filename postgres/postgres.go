package postgres

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/go-pg/pg"
	"github.com/pkg/errors"

	"github.com/unchartedsoftware/deluge/document"
	"github.com/unchartedsoftware/distil-ingest/conf"
	"github.com/unchartedsoftware/distil-ingest/metadata"
	"github.com/unchartedsoftware/distil-ingest/postgres/model"
	"github.com/unchartedsoftware/plog"
)

const (
	metadataTableCreationSQL = `CREATE TABLE %s (
			name	varchar(40)	NOT NULL,
			role	varchar(20),
			type	varchar(20)
		);`
	resultTableCreationSQL = `CREATE TABLE %s (
			result_id	varchar(1000)	NOT NULL,
			index		BIGINT,
			target		varchar(40),
			value		varchar(200)
		);`

	sessionTableName        = "session"
	requestTableName        = "request"
	resultMetadataTableName = "result"
	resultScoreTableName    = "result_score"
	requestFeatureTableName = "request_feature"
	requestFilterTableName  = "request_filter"

	sessionTableCreationSQL = `CREATE TABLE %s (
			session_id	varchar(200)
		);`
	requestTableCreationSQL = `CREATE TABLE %s (
			session_id			varchar(200),
			request_id			varchar(200),
			dataset				varchar(200),
			progress			varchar(40),
			created_time		timestamp,
			last_updated_time	timestamp
		);`
	resultMetadataTableCreationSQL = `CREATE TABLE %s (
			request_id		varchar(200),
			pipeline_id		varchar(200),
			result_uuid		varchar(200),
			result_uri		varchar(200),
			progress		varchar(40),
			output_type		varchar(200),
			created_time	timestamp
		);`
	requestFeatureTableCreationSQL = `CREATE TABLE %s (
			request_id		varchar(200),
			feature_name	varchar(40),
			feature_type	varchar(20)
		);`
	requestFilterTableCreationSQL = `CREATE TABLE %s (
			request_id			varchar(200),
			feature_name		varchar(40),
			filter_type			varchar(40),
			filter_min			double precision,
			filter_max			double precision,
			filter_categories	varchar(200)
		);`
	resultScoreTableCreationSQL = `CREATE TABLE %s (
			pipeline_id	varchar(200),
			metric		varchar(40),
			score		double precision
		);`
)

var (
	nonNullableTypes = map[string]bool{
		"index":   true,
		"integer": true,
		"float":   true,
	}
)

// Database is a struct representing a full logical database.
type Database struct {
	DB        *pg.DB
	Tables    map[string]*model.Dataset
	BatchSize int
}

// NewDatabase creates a new database instance.
func NewDatabase(config *conf.Conf) (*Database, error) {
	db := pg.Connect(&pg.Options{
		User:     config.DBUser,
		Password: config.DBPassword,
		Database: config.Database,
	})

	database := &Database{
		DB:        db,
		Tables:    make(map[string]*model.Dataset),
		BatchSize: config.DBBatchSize,
	}

	return database, nil
}

// CreatePipelineMetadataTables creates an empty table for the pipeline results.
func (d *Database) CreatePipelineMetadataTables() error {
	// Create the pipeline tables.
	log.Infof("Creating pipeline metadata tables.")
	d.DropTable(sessionTableName)
	_, err := d.DB.Exec(fmt.Sprintf(sessionTableCreationSQL, sessionTableName))
	if err != nil {
		return err
	}

	d.DropTable(requestTableName)
	_, err = d.DB.Exec(fmt.Sprintf(requestTableCreationSQL, requestTableName))
	if err != nil {
		return err
	}

	d.DropTable(resultMetadataTableName)
	_, err = d.DB.Exec(fmt.Sprintf(resultMetadataTableCreationSQL, resultMetadataTableName))
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

	d.DropTable(resultScoreTableName)
	_, err = d.DB.Exec(fmt.Sprintf(resultScoreTableCreationSQL, resultScoreTableName))
	if err != nil {
		return err
	}

	return nil
}

func (d *Database) executeInserts(tableName string) error {
	ds := d.Tables[tableName]

	insertStatement := fmt.Sprintf("INSERT INTO %s_base VALUES %s;", tableName, strings.Join(ds.GetBatch(), ", "))

	_, err := d.DB.Exec(insertStatement, ds.GetBatchArgs()...)

	return err
}

// CreateResultTable creates an empty table for the pipeline results.
func (d *Database) CreateResultTable(tableName string) error {
	resultTableName := fmt.Sprintf("%s_result", tableName)

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
	variableTableName := fmt.Sprintf("%s_variable", tableName)

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

// IngestRow parses the raw csv data and stores it to the table specified.
// The previously parsed metadata is used to map columns.
func (d *Database) IngestRow(tableName string, data string) error {
	ds := d.Tables[tableName]

	insertStatement := ""
	variables := ds.Variables
	values := make([]interface{}, len(variables))
	doc := &document.CSV{}
	doc.SetData(data)
	for i := 0; i < len(variables); i++ {
		// Default columns that have an empty column.
		var val interface{}
		if d.isNullVariable(variables[i].Type, doc.Cols[i]) {
			val = nil
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
			err := d.executeInserts(tableName)
			if err != nil {
				return errors.Wrap(err, "unable to insert remaining rows for table "+tableName)
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
	log.Infof("Creating table %s", tableName)

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
func (d *Database) InitializeDataset(meta *metadata.Metadata) (*model.Dataset, error) {
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
	case "float":
		return "FLOAT8"
	case "longitude":
		return "FLOAT8"
	case "latitude":
		return "FLOAT8"
	default:
		return "TEXT"
	}
}

// mapVariable uses the variable type to map a string value to the proper type.
func (d *Database) mapVariable(typ, value string) (interface{}, error) {
	// NOTE: current classification has issues so if numerical, assume float64.
	switch typ {
	case "index":
		if value == "" {
			return nil, nil
		}
		return strconv.ParseInt(value, 10, 32)
	case "integer":
		if value == "" {
			return nil, nil
		}
		return strconv.ParseFloat(value, 64)
	case "float":
		if value == "" {
			return nil, nil
		}
		return strconv.ParseFloat(value, 64)
	case "longitude":
		if value == "" {
			return nil, nil
		}
		return strconv.ParseFloat(value, 64)
	case "latitude":
		if value == "" {
			return nil, nil
		}
		return strconv.ParseFloat(value, 64)
	default:
		return value, nil
	}
}

func (d *Database) defaultValue(typ string) interface{} {
	switch typ {
	case "index":
		return int(0)
	case "integer":
		return float64(0)
	case "float":
		return float64(0)
	case "longitude":
		return float64(0)
	case "latitude":
		return float64(0)
	default:
		return "''"
	}
}

func (d *Database) isNullVariable(typ, value string) bool {
	return value == "" && nonNullableTypes[typ]
}
