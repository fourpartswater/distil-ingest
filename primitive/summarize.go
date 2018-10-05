package primitive

import (
	"encoding/json"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/unchartedsoftware/distil-ingest/primitive/compute/description"
	"github.com/unchartedsoftware/distil-ingest/primitive/compute/result"
	"github.com/unchartedsoftware/distil-ingest/rest"
	"github.com/unchartedsoftware/distil-ingest/util"
)

// SummarizePrimitive will summarize the dataset using a primitive.
func (s *IngestStep) SummarizePrimitive(dataset string, outputPath string) error {
	// create & submit the solution request
	pip, err := description.CreateDukePipeline("wellington", "")
	if err != nil {
		return errors.Wrap(err, "unable to create Duke pipeline")
	}

	datasetURI, err := s.submitPrimitive(dataset, pip)
	if err != nil {
		return errors.Wrap(err, "unable to run Duke pipeline")
	}

	// parse primitive response (token,probability)
	res, err := result.ParseResultCSV(datasetURI)
	if err != nil {
		return errors.Wrap(err, "unable to parse Duke pipeline result")
	}

	tokens := make([]string, len(res)-1)
	for i, v := range res {
		// skip the header
		if i > 0 {
			token, ok := v[0].(string)
			if !ok {
				return errors.Wrap(err, "unable to parse Duke token")
			}
			tokens[i-1] = token
		}
	}

	sum := &rest.SummaryResult{
		Summary: strings.Join(tokens, ", "),
	}

	// output the classification in the expected JSON format
	bytes, err := json.MarshalIndent(sum, "", "    ")
	if err != nil {
		return errors.Wrap(err, "unable to serialize summary result")
	}
	// write to file
	err = util.WriteFileWithDirs(outputPath, bytes, os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "unable to store summary result")
	}

	return nil
}
