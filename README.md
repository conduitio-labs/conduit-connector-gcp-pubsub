# Conduit Connector GCP Pub/Sub and Pub/Sub Lite

## General

The GCP Pub/Sub and Pub/Sub Lite connector is one of [Conduit](https://github.com/ConduitIO/conduit) plugins.
It provides both, a source and destination GCP Pub/Sub and Pub/Sub Lite connector.

To use the Pub/Sub Lite, it is necessary to fill out the `location` configuration field.

Under the hood, the connector uses
[Google Cloud Client Libraries for Go](https://github.com/googleapis/google-cloud-go).

## Prerequisites

- [Go](https://go.dev/) 1.21
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.55.2

## How to build it

Run `make build`.

## Testing

Run `make test` to run all unit and integration tests, as well as an acceptance test. To pass the integration and
acceptance tests, set the next configuration parameters to the environment variables: `GCP_PUBSUB_PRIVATE_KEY`,
`GCP_PUBSUB_CLIENT_EMAIL`, and `GCP_PUBSUB_PROJECT_ID`.

## Source

A source connector represents a receiver for messages from GCP Pub/Sub and Pub/Sub Lite.

To receive messages published to a topic, you must create a **pull subscription** to that topic and add it to
the `subscriptionId` configuration field.

Message key-value attributes are written to the record metadata.

If new messages are sent to the topic while the connector is down, these messages will be received after the connector
is up.

Messages in the topic cannot be deleted or changed. Consequently, all messages have a `OperationCreate` operation.

### Configuration

The user can get the authorization data from a JSON file by the following
instructions: [Getting started with authentication](https://cloud.google.com/docs/authentication/getting-started).

| name             | description                                                                  | required | example                                                                        |
|------------------|------------------------------------------------------------------------------|----------|--------------------------------------------------------------------------------|
| `privateKey`     | private key to auth in a client                                              | true     | -----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG\n-----END PRIVATE KEY-----\n |
| `clientEmail`    | client email to auth in a client                                             | true     | test_user@conduit-pubsub.iam.gserviceaccount.com                               |
| `projectId`      | project id to auth in a client                                               | true     | conduit-pubsub                                                                 |
| `subscriptionId` | subscription name to pull messages                                           | true     | conduit-subscription                                                           |
| `location`       | cloud region or zone where the topic resides (for Pub/Sub Lite service only) | false    | europe-central2-a                                                              |

## Destination

A destination connector represents the message publisher to the Pub/Sub and Pub/Sub Lite.

The record's metadata is added to the message key-value attributes.

### Configuration

The user can get the authorization data from a JSON file by the following
instructions: [Getting started with authentication](https://cloud.google.com/docs/authentication/getting-started).

| name          | description                                                                   | required | example                                                                        |
|---------------|-------------------------------------------------------------------------------|----------|--------------------------------------------------------------------------------|
| `privateKey`  | private key to auth in a client                                               | true     | -----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG\n-----END PRIVATE KEY-----\n |
| `clientEmail` | client email to auth in a client                                              | true     | test_user@conduit-pubsub.iam.gserviceaccount.com                               |
| `projectId`   | project id to auth in a client                                                | true     | conduit-pubsub                                                                 |
| `topicId`     | topic name to push messages                                                   | true     | conduit-topic                                                                  |
| `location`    | cloud region or zone where the topic resides (for Pub/Sub Lite service only)  | false    | europe-central2-a                                                              |

### Quotas and limits
- [Pub/Sub](https://cloud.google.com/pubsub/quotas)
- [Pub/Sub Lite](https://cloud.google.com/pubsub/lite/quotas)