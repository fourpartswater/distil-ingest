package primitive

import (
	"encoding/json"
	"os"
	"strconv"

	"github.com/pkg/errors"
	"github.com/unchartedsoftware/distil-ingest/rest"

	"github.com/unchartedsoftware/distil-compute/model"
	"github.com/unchartedsoftware/distil-compute/primitive/compute/description"
	"github.com/unchartedsoftware/distil-compute/primitive/compute/result"
	"github.com/unchartedsoftware/distil-ingest/util"
)

// ClassifyPrimitive will classify the dataset using a primitive.
func (s *IngestStep) Classify(dataset string, outputPath string) error {
	// create & submit the solution request
	pip, err := description.CreateSimonPipeline("says", "")
	if err != nil {
		return errors.Wrap(err, "unable to create Simon pipeline")
	}

	datasetURI, err := s.submitPrimitive([]string{dataset}, pip)
	if err != nil {
		return errors.Wrap(err, "unable to run Simon pipeline")
	}

	// parse primitive response (variable,probabilities,labels)
	res, err := result.ParseResultCSV(datasetURI)
	if err != nil {
		return errors.Wrap(err, "unable to parse Simon pipeline result")
	}

	// First row is header, then all other rows are col index, types, probabilities.
	probabilities := make([][]float64, len(res)-1)
	labels := make([][]string, len(res)-1)
	for i, v := range res {
		if i > 0 {
			colIndex, err := strconv.ParseInt(v[0].(string), 10, 64)
			if err != nil {
				return err
			}
			fieldLabels := toStringArray(v[1].([]interface{}))
			labels[colIndex] = mapClassifiedTypes(fieldLabels)
			probs, err := toFloat64Array(v[2].([]interface{}))
			if err != nil {
				return err
			}
			probabilities[colIndex] = probs
		}
	}
	classification := &rest.ClassificationResult{
		Path:          datasetURI,
		Labels:        labels,
		Probabilities: probabilities,
	}

	// output the classification in the expected JSON format
	bytes, err := json.MarshalIndent(classification, "", "    ")
	if err != nil {
		return errors.Wrap(err, "unable to serialize classification result")
	}
	// write to file
	err = util.WriteFileWithDirs(outputPath, bytes, os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "unable to store classification result")
	}

	return nil
}

func mapClassifiedTypes(types []string) []string {
	for i, typ := range types {
		types[i] = model.MapSimonType(typ)
	}

	return types
}
