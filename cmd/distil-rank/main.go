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

package main

import (
	"bytes"
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/pkg/errors"
	log "github.com/unchartedsoftware/plog"
	"github.com/urfave/cli"

	"github.com/uncharted-distil/distil-compute/primitive/compute"
	"github.com/uncharted-distil/distil-ingest/primitive"
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
	app.Name = "distil-rank"
	app.Version = "0.1.0"
	app.Usage = "Rank D3M merged datasets"
	app.UsageText = "distil-rank --endpoint=<url> --dataset=<filepath> --output=<filepath>"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "schema",
			Value: "",
			Usage: "The dataset schema file path",
		},
		cli.StringFlag{
			Name:  "type-source",
			Value: "schema",
			Usage: "The source for the type information, either `schema` or `classification`",
		},
		cli.StringFlag{
			Name:  "dataset",
			Value: "",
			Usage: "The dataset source path",
		},
		cli.StringFlag{
			Name:  "classification",
			Value: "",
			Usage: "The classification source path",
		},
		cli.BoolFlag{
			Name:  "has-header",
			Usage: "Whether or not the CSV file has a header row",
		},
		cli.StringFlag{
			Name:  "endpoint",
			Value: "",
			Usage: "The pipeline runner endpoint",
		},
		cli.StringFlag{
			Name:  "output",
			Value: "",
			Usage: "The ranking output file path",
		},
		cli.StringFlag{
			Name:  "ranking-output",
			Value: "",
			Usage: "The numeric output file path to use for for the file to rank",
		},
		cli.IntFlag{
			Name:  "row-limit",
			Value: 1000,
			Usage: "The number of rows to send to the ranking system",
		},
	}
	app.Action = func(c *cli.Context) error {

		if c.String("dataset") == "" {
			return cli.NewExitError("missing commandline flag `--dataset`", 1)
		}
		if c.String("endpoint") == "" {
			return cli.NewExitError("missing commandline flag `--endpoint`", 1)
		}
		if c.String("output") == "" {
			return cli.NewExitError("missing commandline flag `--ranking-output`", 1)
		}

		//classificationPath := filepath.Clean(c.String("classification"))
		//typeSource := c.String("type-source")
		//schemaPath := filepath.Clean(c.String("schema"))
		endpoint := c.String("endpoint")
		datasetPath := filepath.Clean(c.String("dataset"))
		//rankingOutputFile := c.String("ranking-output")
		//rowLimit := c.Int("row-limit")
		//hasHeader := c.Bool("has-header")
		outputFilePath := c.String("output")

		// initialize client
		log.Infof("Using pipeline runner interface at `%s` ", endpoint)
		client, err := compute.NewRunner(endpoint, true, "distil-ingest", 60, 10, true)
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}
		step := primitive.NewIngestStep(client)

		// rank the dataset variable importance
		err = step.Rank(datasetPath, outputFilePath)
		if err != nil {
			log.Errorf("%v", err)
			return cli.NewExitError(errors.Cause(err), 2)
		}
		log.Infof("Ranked data written to %s", outputFilePath)

		return nil
	}
	// run app
	app.Run(os.Args)
}

func getMergedData(header []string, datasetPath string, hasHeader bool, rowLimit int) ([]byte, error) {
	// Copy source to destination.
	file, err := os.Open(datasetPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open source file")
	}

	reader := csv.NewReader(file)

	// output writer
	output := &bytes.Buffer{}
	writer := csv.NewWriter(output)
	if header != nil && len(header) > 0 {
		err := writer.Write(header)
		if err != nil {
			return nil, errors.Wrap(err, "failed to write header to file")
		}
	}

	count := 0
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, errors.Wrap(err, "failed to read line from file")
		}
		if (count > 0 || !hasHeader) && count < rowLimit {
			err := writer.Write(line)
			if err != nil {
				return nil, errors.Wrap(err, "failed to write line to file")
			}
		}
		count++
	}
	// flush writer
	writer.Flush()

	// close left
	err = file.Close()
	if err != nil {
		return nil, errors.Wrap(err, "failed to close input file")
	}
	return output.Bytes(), nil
}
