package schema

import "github.com/ONSdigital/dp-kafka/v2/avro"

var inputFileAvailable = `{
  "type": "record",
  "name": "input-file-available",
  "fields": [
    {"name": "file_url", "type": "string"},
    {"name": "instance_id", "type": "string"}
  ]
}`

// InputFileAvailableSchema is the Avro schema for each
// input file that becomes available
var InputFileAvailableSchema *avro.Schema = &avro.Schema{
	Definition: inputFileAvailable,
}

var dimensionsExtracted = `{
  "type": "record",
  "name": "dimensions-extracted",
  "fields": [
    {"name": "file_url", "type": "string"},
    {"name": "instance_id", "type": "string"}
  ]
}`

// DimensionsExtractedSchema is the Avro schema for each dimension extracted
var DimensionsExtractedSchema *avro.Schema = &avro.Schema{
	Definition: dimensionsExtracted,
}
