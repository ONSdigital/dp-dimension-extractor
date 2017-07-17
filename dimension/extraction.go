package dimension

import "fmt"

// ----------------------------------------------------------------------------

// Extract represents an extract details
type Extract struct {
	Dimensions            map[string]string
	DimensionColumnOffset int
	ImportAPIURL          string
	InstanceID            string
	Line                  []string
}

// InvalidNumberOfColumns is returned when the number of columns is not divisible by 3
type InvalidNumberOfColumns struct {
	line []string
}

func (e *InvalidNumberOfColumns) Error() string {
	return fmt.Sprintf("invalid number of columns: [%d], needs to be divisible by 2", len(e.line))
}

// ----------------------------------------------------------------------------

func New(dimensions map[string]string, dimensionColumnOffset int, importAPIURL string, instanceID string, line []string) *Extract {
	return &Extract{
		Dimensions:            dimensions,
		DimensionColumnOffset: dimensionColumnOffset,
		ImportAPIURL:          importAPIURL,
		InstanceID:            instanceID,
		Line:                  line,
	}
}

// ----------------------------------------------------------------------------

// Extract method retrieves dimensions and if unique calls the sendRequest method
func (extract *Extract) Extract() (map[string]Request, error) {
	dimensions := make(map[string]Request)
	line := extract.Line

	// dimensionColumnOffset is the number of columns that exist
	// prior to the columns that are relevant to the dimensions
	dimensionColumnOffset := extract.DimensionColumnOffset + 1

	// dimensionColumns is the number of columns that make a
	// unique dimension
	dimensionColumns := 2

	// Check the number of columns is the expected amount
	if (len(line)-dimensionColumnOffset)%dimensionColumns != 0 {
		return nil, &InvalidNumberOfColumns{line}
	}

	for i := dimensionColumnOffset; i < len(line); i += dimensionColumns {

		dimension := extract.InstanceID + "_" + line[i] + "_" + line[i+1]
		dimensionValue := line[i+1]
		dimensionAlreadyExists := false

		// If dimension already exists add dimension to map
		if _, ok := extract.Dimensions[dimension]; ok {
			dimensionAlreadyExists = true
		}

		if !dimensionAlreadyExists {
			extract.Dimensions[dimension] = dimension

			request := Request{
				Dimension:      dimension,
				DimensionValue: dimensionValue,
				ImportAPIURL:   extract.ImportAPIURL,
				InstanceID:     extract.InstanceID,
			}

			dimensions[dimension] = request
		}
	}

	return dimensions, nil
}
