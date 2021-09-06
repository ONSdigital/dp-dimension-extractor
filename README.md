# dp-dimension-extractor

Handles inserting of dimensions into database after input file becomes available;
and creates an event by sending a message to a dimension-extracted kafka topic so further processing of the input file can take place.

1. Consumes from the INPUT_FILE_AVAILABLE_TOPIC
2. Retrieves file (csv) from aws S3 bucket
3. Put requests for each unique dimension onto database via the dataset API
4. Produces a message to the DIMENSIONS_EXTRACTED_TOPIC

## Requirements

In order to run the service locally you will need the following:

- [Go](https://golang.org/doc/install)
- [Git](https://git-scm.com/downloads)
- [Kafka](https://kafka.apache.org/)
- [Dataset API](https://github.com/ONSdigital/dp-dataset-api)
- [API AUTH STUB](https://github.com/ONSdigital/dp-auth-api-stub)
- [Vault](https://www.vaultproject.io/)

To run vault:

- Run `brew install vault`
- Run `vault server -dev`

## Getting started

* Clone the repo `go get github.com/ONSdigital/dp-dimension-extractor`
* Run kafka and zookeeper
* Run local S3 store
* Run the dataset API, see documentation [here](https://github.com/ONSdigital/dp-dataset-api)
* Run api auth stub, see documentation [here](https://github.com/ONSdigital/dp-auth-api-stub)
* Run the application with `make debug`

### Kafka scripts

Scripts for updating and debugging Kafka can be found [here](https://github.com/ONSdigital/dp-data-tools)(dp-data-tools)

### Configuration

| Environment variable         | Default                               | Description
| ---------------------------- | ------------------------------------- | ----------------------------------------------------
| AWS_REGION                   | eu-west-1                             | The AWS region to use
| BIND_ADDR                    | :21400                                | The host and port to bind to
| DATASET_API_URL              | http://localhost:22000                | The dataset API url
| DATASET_API_AUTH_TOKEN       | FD0108EA-825D-411C-9B1D-41EF7727F465  | Authentication token for access to dataset API
| DIMENSIONS_EXTRACTED_TOPIC   | dimensions-extracted                  | The kafka topic to write messages to
| DIMENSION_EXTRACTOR_URL      | http://localhost:21400                | The dimension extractor url
| ENCRYPTION_DISABLED          | true                                  | A boolean flag to identify if encryption of files is disabled or not
| EVENT_REPORTER_TOPIC         | report-events                         | The kafka topic to send errors to
| GRACEFUL_SHUTDOWN_TIMEOUT    | 5s                                    | The graceful shutdown timeout in seconds
| INPUT_FILE_AVAILABLE_GROUP   | input-file-available                  | The kafka consumer group to consume messages from
| INPUT_FILE_AVAILABLE_TOPIC   | input-file-available                  | The kafka topic to consume messages from
| KAFKA_ADDR                   | localhost:9092                        | The kafka broker addresses (can be comma separated)
| KAFKA_MAX_BYTES              | 2000000                               | The maximum permitted size of a message. Should be set equal to or smaller than the broker's `message.max.bytes`
| KAFKA_VERSION                | "1.0.2"                               | The kafka version that this service expects to connect to
| KAFKA_SEC_PROTO              | _unset_                               | if set to `TLS`, kafka connections will use TLS [[1]](#notes_1)
| KAFKA_SEC_CLIENT_KEY         | _unset_                               | PEM for the client key [[1]](#notes_1)
| KAFKA_SEC_CLIENT_CERT        | _unset_                               | PEM for the client certificate [[1]](#notes_1)
| KAFKA_SEC_CA_CERTS           | _unset_                               | CA cert chain for the server cert [[1]](#notes_1)
| KAFKA_SEC_SKIP_VERIFY        | false                                 | ignores server certificate issues if `true` [[1]](#notes_1)
| REQUEST_MAX_RETRIES          | 3                                     | The maximum number of attempts for a single http request due to external service failure"
| VAULT_ADDR                   | http://localhost:8200                 | The vault address
| VAULT_TOKEN                  | -                                     | Vault token required for the client to talk to vault. (Use `make debug` to create a vault token)
| VAULT_PATH                   | secret/shared/psk                     | The path where the psks will be stored in for vault
| SERVICE_AUTH_TOKEN           | E45F9BFC-3854-46AE-8187-11326A4E00F4  | The service authorization token
| ZEBEDEE_URL                  | http://localhost:8082                 | The host name for Zebedee
| AWS_ACCESS_KEY_ID            | -                                     | The AWS access key credential for the dimension extractor
| AWS_SECRET_ACCESS_KEY        | -                                     | The AWS secret key credential for the dimension extractor
| HEALTHCHECK_INTERVAL         | 30s                                   | The period of time between health checks
| HEALTHCHECK_CRITICAL_TIMEOUT | 90s                                   | The period of time after which failing checks will result in critical global check

**Notes:**

1. <a name="notes_1">For more info, see the [kafka TLS examples documentation](https://github.com/ONSdigital/dp-kafka/tree/main/examples#tls)</a>

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright © 2016-2021, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details.