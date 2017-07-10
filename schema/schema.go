package schema

// InputFileAvailable schema
var InputFileAvailable = `{
  "type": "record",
  "name": "input-file-available",
  "fields": [
    {"name": "file_url", "type": "string"},
    {"name": "instance_id", "type": "string"}
  ]
}`

// DimensionsExtracted schema
var DimensionsExtracted = `{
  "type": "record",
  "name": "dimensions-extracted",
  "fields": [
    {"name": "file_url", "type": "string"},
    {"name": "instance_id", "type": "string"}
  ]
}`
