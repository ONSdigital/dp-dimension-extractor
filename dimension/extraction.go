package dimension

import "fmt"

// Extract represents all information needed to extract dimensions
type Extract struct {
	Dimensions            map[string]string
	DimensionColumnOffset int
	ImportAPIURL          string
	InstanceID            string
	Line                  []string
	MaxRetries            int
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

// New returns a new Extract object for a given instance
func New(dimensions map[string]string, dimensionColumnOffset int, importAPIURL string, instanceID string, line []string, maxRetries int) *Extract {
	return &Extract{
		Dimensions:            dimensions,
		DimensionColumnOffset: dimensionColumnOffset,
		ImportAPIURL:          importAPIURL,
		InstanceID:            instanceID,
		Line:                  line,
		MaxRetries:            maxRetries,
	}
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

		dimension := extract.InstanceID + "_" + line[i] + "_" + line[i+1]
		dimensionValue := line[i+1]

		// If dimension already exists add dimension to map
		if _, ok := extract.Dimensions[dimension]; ok {
			continue
		}

		extract.Dimensions[dimension] = dimension

		request := Request{
			Attempt:        1,
			Dimension:      dimension,
			DimensionValue: dimensionValue,
			ImportAPIURL:   extract.ImportAPIURL,
			InstanceID:     extract.InstanceID,
			MaxAttempts:    extract.MaxRetries,
		}

		dimensions[dimension] = request
	}

	return dimensions, nil
}
