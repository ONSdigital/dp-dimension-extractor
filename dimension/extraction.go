package dimension

import (
	"errors"
	"fmt"
	"strings"

	dataset "github.com/ONSdigital/dp-api-clients-go/dataset"
)

// Extract represents all information needed to extract dimensions
type Extract struct {
	DimensionColumnOffset int
	HeaderRow             []string
	InstanceID            string
	Line                  []string
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

// MissingDimensionValues returns a list of missing dimension values
type MissingDimensionValues struct {
	Line []string
}

func (e *MissingDimensionValues) Error() string {
	return fmt.Sprintf("missing dimension values in : [%v]", e.Line)
}

// Extract method checks within csv line for unique dimensions from the consumed dataset and returns them
func (extract *Extract) Extract() (map[string]dataset.OptionPost, error) {
	dimensionOptions := make(map[string]dataset.OptionPost)
	line := extract.Line

	// dimensionColumnOffset is the number of columns that exist
	// prior to the columns that are relevant to the dimensions
	dimensionColumnOffset := extract.DimensionColumnOffset

	// Check the number of columns is the expected amount
	if (len(line)-dimensionColumnOffset)%dimensionColumns != 0 {
		return nil, &InvalidNumberOfColumns{line}
	}

	// create dataset dimension option with first column line as code
	// if code is not present, a MissingDimensionValues error will be returned
	for i := dimensionColumnOffset; i < len(line); i += dimensionColumns {
		if len(line[i]) == 0 {
			return nil, &MissingDimensionValues{line}
		}

		dimensionValue := line[i]
		dimensionName := extract.HeaderRow[i+1]
		dimensionCodeList, ok := extract.CodelistMap[strings.ToLower(dimensionName)]
		if !ok {
			return nil, errors.New("Failed to map dimension to code list, " + dimensionName)
		}

		optionToPost := dataset.OptionPost{
			Name:     strings.ToLower(dimensionName),
			Option:   dimensionValue,
			Label:    line[i+1],
			CodeList: dimensionCodeList,
			Code:     line[i],
		}

		dimensionOptionKey := dimensionName + "_" + dimensionValue
		dimensionOptions[dimensionOptionKey] = optionToPost
	}
	return dimensionOptions, nil
}
