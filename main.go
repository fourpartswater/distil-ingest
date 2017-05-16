package main

import (
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/unchartedsoftware/d3m-ingest/conf"
	"github.com/unchartedsoftware/d3m-ingest/document/d3mdata"
	"github.com/unchartedsoftware/deluge"
	"github.com/unchartedsoftware/plog"
	"gopkg.in/olivere/elastic.v3"
)

const (
	timeout       = time.Second * 60 * 5
	errSampleSize = 10
)

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	// parse flags into config struct
	config, err := conf.ParseCommandLine()
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}

	// Filesystem Input
	input, err := deluge.NewFileInput(config.FileInputPath, config.FileInputExcludes)

	// create elasticsearch client
	client, err := elastic.NewClient(
		elastic.SetURL(config.ESEndpoint),
		elastic.SetHttpClient(&http.Client{Timeout: timeout}),
		elastic.SetMaxRetries(10),
		elastic.SetSniff(false),
		elastic.SetGzip(true))
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}

	doc := d3mdata.NewD3MData(config.FileInputPath + "/dataSchema.json")

	// create ingestor
	ingestor, err := deluge.NewIngestor(
		deluge.SetDocument(doc),
		deluge.SetInput(input),
		deluge.SetClient(client),
		deluge.SetIndex(config.ESIndex),
		deluge.SetErrorThreshold(config.ErrThreshold),
		deluge.SetActiveConnections(config.NumActiveConnections),
		deluge.SetNumWorkers(config.NumWorkers),
		deluge.SetBulkByteSize(config.BulkByteSize),
		deluge.SetScanBufferSize(config.ScanBufferSize),
		deluge.ClearExistingIndex(config.ClearExisting),
		deluge.SetNumReplicas(1))
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}

	// ingest
	err = ingestor.Ingest()
	if err != nil {
		log.Error(err)
	}

	// check errors
	errs := deluge.DocErrs()
	if len(errs) > 0 {
		log.Errorf("Failed ingesting %d documents, logging sample size of %d errors:",
			len(errs),
			errSampleSize)
		for _, err := range deluge.SampleDocErrs(errSampleSize) {
			log.Error(err)
		}
	}
}