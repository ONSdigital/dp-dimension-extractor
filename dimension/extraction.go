package dimension

import (
	"errors"
	"fmt"
	"strings"
)

// Extract represents all information needed to extract dimensions
type Extract struct {
	Dimensions            map[string]string
	DimensionColumnOffset int
	HeaderRow             []string
	DatasetAPIURL         string
	DatasetAPIAuthToken   string
	InstanceID            string
	Line                  []string
	MaxRetries            int
	TimeColumn            int
	CodelistMap           map[string]string
}

// dimensionColumns is the number of columns that make a unique dimension
const dimensionColumns = 2

// InvalidNumberOfColumns is returned when the number of columns is not divisible by 2
type InvalidNumberOfColumns struct {
	Line []string
}

func (e *InvalidNumberOfColumns) Error() string {
	return fmt.Sprintf("invalid number of columns: [%d], needs to be divisible by 2", len(e.Line))
}

// InvalidNumberOfColumns is returned when the number of columns is not divisible by 2
type MissingDimensionValues struct {
	Line []string
}

func (e *MissingDimensionValues) Error() string {
	return fmt.Sprintf("missing dimension values in : [%v]", e.Line)
}

// Extract method checks within csv line for unique dimensions from the consumed dataset and returns them
func (extract *Extract) Extract() (map[string]Request, error) {
	dimensions := make(map[string]Request)
	line := extract.Line

	// dimensionColumnOffset is the number of columns that exist
	// prior to the columns that are relevant to the dimensions
	dimensionColumnOffset := extract.DimensionColumnOffset

	// Check the number of columns is the expected amount
	if (len(line)-dimensionColumnOffset)%dimensionColumns != 0 {
		return nil, &InvalidNumberOfColumns{line}
	}

	for i := dimensionColumnOffset; i < len(line); i += dimensionColumns {
		var dimensionValue string

		// If a pair of columns represents time always use the value in the second
		// column to represent dimension value
		if i == extract.TimeColumn || i+1 == extract.TimeColumn {
			dimensionValue = line[i+1]
		} else {
			// For all other dimensons use first column in pair; if this is empty use
			// second column and lastly if both columns are empty then throw an error
			if len(line[i]) > 0 {
				dimensionValue = line[i]
			} else {
				if len(line[i+1]) > 0 {
					dimensionValue = line[i+1]
				} else {
					return nil, &MissingDimensionValues{line}
				}
			}
		}

		dimension := extract.InstanceID + "_" + extract.HeaderRow[i+1]

		// If dimension already exists add dimension to map
		if _, ok := extract.Dimensions[dimension+"_"+dimensionValue]; ok {
			continue
		}

		dimensionCodeList, ok := extract.CodelistMap[strings.ToLower(extract.HeaderRow[i+1])]
		if !ok {
			return nil, errors.New("Failed to map dimension to code list, " + extract.HeaderRow[i+1])
		}

		extract.Dimensions[dimension+"_"+dimensionValue] = dimension

		request := Request{
			Attempt:             1,
			DimensionID:         strings.ToLower(extract.HeaderRow[i+1]),
			Code:                line[i],
			Value:               dimensionValue,
			Label:               line[i+1],
			CodeList:            dimensionCodeList,
			DatasetAPIURL:       extract.DatasetAPIURL,
			DatasetAPIAuthToken: extract.DatasetAPIAuthToken,
			InstanceID:          extract.InstanceID,
			MaxAttempts:         extract.MaxRetries,
		}

		dimensions[dimension] = request
	}

	return dimensions, nil
}
