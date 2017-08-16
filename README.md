dp-dimension-extractor
================

Handles inserting of dimensions into database after input file becomes available;
and creates an event by sending a message to a dimension-extracted kafka topic so further processing of the input file can take place.

1. Consumes from the INPUT_FILE_AVAILABLE_TOPIC
2. Retrieves file (csv) from aws S3 bucket
3. Put requests for each unique dimension onto database via import API
4. Produces a message to the DIMENSIONS_EXTRACTED_TOPIC

Requirements
-----------------
In order to run the service locally you will need the following:
- [Go](https://golang.org/doc/install)
- [Git](https://git-scm.com/downloads)
- [Kafka](https://kafka.apache.org/)
- [Import API](https://github.com/ONSdigital/dp-import-api)

### Getting started

* Clone the repo `go get github.com/ONSdigital/dp-dimension-extractor`
* Run kafka and zookeeper
* Run local S3 store
* Run import API, see documentation [here](https://github.com/ONSdigital/dp-import-api)
* Run the application `make debug`

### Configuration

| Environment variable         | Default                               | Description
| ---------------------------- | ------------------------------------- | ----------------------------------------------------
| AWS_REGION                   | eu-west-1                             | The AWS region to use
| BIND_ADDR                    | :21400                                | The host and port to bind to
| DIMENSIONS_EXTRACTED_TOPIC   | dimensions-extracted                  | The kafka topic to write messages to
| IMPORT_API_URL               | http://localhost:21800                | The import api url
| IMPORT_AUTH_TOKEN            | FD0108EA-825D-411C-9B1D-41EF7727F465  | Authentication token for access to import API
| INPUT_FILE_AVAILABLE_GROUP   | input-file-available                  | The kafka consumer group to consume messages from
| INPUT_FILE_AVAILABLE_TOPIC   | input-file-available                  | The kafka topic to consume messages from
| KAFKA_ADDR                   | localhost:9092                        | The kafka broker addresses (can be comma separated)
| KAFKA_MAX_BYTES              | 2000000                               | The maximum permitted size of a message. Should be set equal to or smaller than the broker's `message.max.bytes`
| REQUEST_MAX_RETRIES          | 3                                     | The maximum number of attempts for a single http request due to external service failure"

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright © 2016-2017, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details.
