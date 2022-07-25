# Conduit Connector Google Cloud Platform Pub/Sub

### General
The GCP Pub/Sub connector is one of [Conduit](https://github.com/ConduitIO/conduit) plugins. It provides both, a source and destination GCP Pub/Sub connector.

### Prerequisites
- [Go](https://go.dev/) 1.18
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.46.2

### How to build it
Run `make build`.

### Testing
Run `make test`.

### How it works
Under the hood, the connector uses [Google Cloud Client Libraries for Go](https://github.com/googleapis/google-cloud-go).

### Source
A source connector represents the receiver of the messages from the GCP Pub/Sub.

`Configure` parses the configuration and validates them.

`Open` initializes the GCP Pub/Sub client and calls the client's `Receive` method.

`Receive` method takes a callback function, which is called each time a message is received.

The callback function sends messages to the queue and `Read` method receives messages from this queue.

`Ack` calls the acknowledge method once the message was received.

`Teardown` marks all unread messages from the queue that the client has not received and releases the GCP subscriber client.

#### Configuration
The user can get the authorization data from a JSON file by the following instructions: [Getting started with authentication](https://cloud.google.com/docs/authentication/getting-started).

| name             | description                        | required | example                                                                        |
|------------------|------------------------------------|----------|--------------------------------------------------------------------------------|
| `privateKey`     | private key to auth in a client    | true     | -----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG\n-----END PRIVATE KEY-----\n |
| `clientEmail`    | client email to auth in a client   | true     | test_user@conduit-pubsub.iam.gserviceaccount.com                               |
| `projectId`      | project id to auth in a client     | true     | conduit-pubsub                                                                 |
| `subscriptionId` | subscription name to pull messages | true     | conduit-subscription                                                           |

### Destination
A destination connector represents an **asynchronous** writes to the GCP Pub/Sub.

`Configure` parses the configuration and validates them.

`Open` initializes the GCP Pub/Sub client.

`WriteAsync` publishes the record to the GCP Pub/Sub topic asynchronously. Messages are batched and sent according to `batchSize` and `batchDelay` parameters in the configuration settings.

`Flush` does nothing, because the system does not cache messages before sending them.

`Teardown` cancels the context, sends all remaining published messages, and releases the GCP Pub/Sub client.

#### Configuration
The user can get the authorization data from a JSON file by the following instructions: [Getting started with authentication](https://cloud.google.com/docs/authentication/getting-started).

| name          | description                                                                                    | required | example                                                                        |
|---------------|------------------------------------------------------------------------------------------------|----------|--------------------------------------------------------------------------------|
| `privateKey`  | private key to auth in a client                                                                | true     | -----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG\n-----END PRIVATE KEY-----\n |
| `clientEmail` | client email to auth in a client                                                               | true     | test_user@conduit-pubsub.iam.gserviceaccount.com                               |
| `projectId`   | project id to auth in a client                                                                 | true     | conduit-pubsub                                                                 |
| `topicId`     | topic name to push messages                                                                    | true     | conduit-topic                                                                  |
| `batchSize`   | the size of the batch of messages, on completing which the batch of messages will be published | false    | 10                                                                             |
| `batchDelay`  | the time delay, after which the batch of messages will be published                            | false    | 100ms                                                                          |

### Known limitations
The maximum message size in a publish request is **10Mb**.

[Pub/Sub quotas and limits](https://cloud.google.com/pubsub/quotas)