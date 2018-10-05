package main

import (
	"os"
	"path"
	"runtime"
	"strings"

	"github.com/pkg/errors"
	"github.com/unchartedsoftware/plog"
	"github.com/urfave/cli"

	"github.com/unchartedsoftware/distil-ingest/primitive"
	"github.com/unchartedsoftware/distil-ingest/primitive/compute"
	"github.com/unchartedsoftware/distil-ingest/util"
)

func splitAndTrim(arg string) []string {
	var res []string
	if arg == "" {
		return res
	}
	split := strings.Split(arg, ",")
	for _, str := range split {
		res = append(res, strings.TrimSpace(str))
	}
	return res
}

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	app := cli.NewApp()
	app.Name = "distil-featurize"
	app.Version = "0.1.0"
	app.Usage = "Featurize D3M datasets"
	app.UsageText = "distil-featurize --endpoint=<url> --dataset=<filepath> --output=<filepath>"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "endpoint",
			Value: "",
			Usage: "The pipeline runner endpoint",
		},
		cli.StringFlag{
			Name:  "dataset",
			Value: "",
			Usage: "The dataset source path",
		},
		cli.StringFlag{
			Name:  "schema",
			Value: "",
			Usage: "The schema source path",
		},
		cli.StringFlag{
			Name:  "filetype",
			Value: "csv",
			Usage: "The dataset file type",
		},
		cli.StringFlag{
			Name:  "output",
			Value: "",
			Usage: "The featurize output file path",
		},
		cli.StringFlag{
			Name:  "media-path",
			Value: "",
			Usage: "The path to the folder containing the media subfolder that is accessible for featurization",
		},
		cli.StringFlag{
			Name:  "output-schema",
			Value: "",
			Usage: "The path to use as output for the featurized schema document",
		},
		cli.StringFlag{
			Name:  "output-data",
			Value: "",
			Usage: "The path to use as output for the featurized data",
		},
		cli.BoolFlag{
			Name:  "has-header",
			Usage: "Whether or not the CSV file has a header row",
		},
		cli.Float64Flag{
			Name:  "threshold",
			Value: 0.2,
			Usage: "Confidence threshold to use for labels",
		},
	}
	app.Action = func(c *cli.Context) error {
		if c.String("endpoint") == "" {
			return cli.NewExitError("missing commandline flag `--endpoint`", 1)
		}
		if c.String("dataset") == "" {
			return cli.NewExitError("missing commandline flag `--dataset`", 1)
		}

		endpoint := c.String("endpoint")
		datasetPath := c.String("dataset")
		//mediaPath := c.String("media-path")
		outputSchema := c.String("output-schema")
		//outputData := c.String("output-data")
		schemaPath := c.String("schema")
		outputFilePath := c.String("output")
		hasHeader := c.Bool("has-header")
		//threshold := c.Float64("threshold")

		// initialize client
		log.Infof("Using pipeline runner interface at `%s` ", endpoint)
		client, err := compute.NewRunner(endpoint, true, "distil-ingest", 60, 10, true)
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}
		step := primitive.NewIngestStep(client)

		// create feature folder
		featurePath := path.Join(outputFilePath, "features")
		if util.DirExists(featurePath) {
			// delete existing data to overwrite with latest
			os.RemoveAll(featurePath)
			log.Infof("Deleted data at %s", featurePath)
		}
		if err := os.MkdirAll(featurePath, 0777); err != nil && !os.IsExist(err) {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}
		os.Remove(path.Join(outputFilePath, "featureDatasetDoc.json"))

		// create featurizer
		err = step.FeaturizePrimitive(schemaPath, datasetPath, datasetPath, outputSchema, outputFilePath, hasHeader)
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}
		log.Infof("Featurized data written to %s", outputFilePath)

		return nil
	}
	// run app
	app.Run(os.Args)
}
