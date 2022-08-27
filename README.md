# Conduit Connector Google Cloud Platform Pub/Sub

### General
The GCP Pub/Sub connector is one of [Conduit](https://github.com/ConduitIO/conduit) plugins. 
It provides both, a source and destination GCP Pub/Sub connector.

The connector supports [Pub/Sub Lite](https://cloud.google.com/pubsub/lite/docs) service.
To use the Pub/Sub Lite service, it is necessary to fill out the `location` configuration field.

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
A source connector represents the receiver of the messages from the GCP Pub/Sub or Pub/Sub Lite services.

#### How it works
The system contains two queues in memory.

The first queue contains records witch were returned by the `Read` method.
Messages are continuously added to this queue as soon as they appear in the topic.

The second queue exists to acknowledge records using the `Ack` method. 
Messages are added to this queue immediately after a record is returned by the Read method.

If new messages are sent to GCP Pub/Sub while the connector is down, 
these messages will be received after the connector is up.

**CDC**: Messages that are in the service cannot be deleted or changed. 
Consequently, all messages have no `action` key in the metadata.

Messages can store own metadata as a key value data.
All message metadata is passed to the record metadata.

#### Methods
`Configure` parses the configuration and validates them.

`Open` initializes the client and calls the client's `Receive` method.

`Receive` method takes a callback function, which is called each time a message is received.

The callback function sends messages to the queue and `Read` method receives messages from this queue.

`Ack` calls the acknowledge method once the message was received.

`Teardown` marks all unread messages from the queue that the client has not received (for Pub/Sub) and releases the client.

#### Configuration
The user can get the authorization data from a JSON file by the following instructions: [Getting started with authentication](https://cloud.google.com/docs/authentication/getting-started).

| name             | description                                                                  | required | example                                                                        |
|------------------|------------------------------------------------------------------------------|----------|--------------------------------------------------------------------------------|
| `privateKey`     | private key to auth in a client                                              | true     | -----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG\n-----END PRIVATE KEY-----\n |
| `clientEmail`    | client email to auth in a client                                             | true     | test_user@conduit-pubsub.iam.gserviceaccount.com                               |
| `projectId`      | project id to auth in a client                                               | true     | conduit-pubsub                                                                 |
| `subscriptionId` | subscription name to pull messages                                           | true     | conduit-subscription                                                           |
| `location`       | cloud region or zone where the topic resides (for Pub/Sub Lite service only) | false    | europe-central2-a                                                              |
**Notes**:
1. The source connector supports subscriptions with **pull** delivery type only.
2. Each subscription receives only one time a message from the topic. 
So if you need to get one message sent to a topic twice (or more) - create two (or more) subscriptions and connectors to them.

### Destination
A destination connector represents an **asynchronous** writes to the Pub/Sub or Pub/Sub Lite services.

`Configure` parses the configuration and validates them.

`Open` initializes the client.

`Write` publishes records to the topic.

`Teardown` cancels the context, sends all remaining published messages, and releases the client.

#### Configuration
The user can get the authorization data from a JSON file by the following instructions: [Getting started with authentication](https://cloud.google.com/docs/authentication/getting-started).

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