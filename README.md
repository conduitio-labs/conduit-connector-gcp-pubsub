# Conduit Connector Google Cloud Platform Pub/Sub

### General
The GCP Pub/Sub connector is one of [Conduit](https://github.com/ConduitIO/conduit) plugins. It provides both, a source and destination GCP Pub/Sub connector.

### Prerequisites
- [Go](https://go.dev/) 1.18
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.45.2

### How it works

Under the hood, the connector uses [Google Cloud Client Libraries for Go](https://github.com/googleapis/google-cloud-go).

### Source

A GCP Pub/Sub source connector represents the receiver of the messages.

`Open` initializes the GCP subscriber client and calls the client's `Receive` method.

`Receive` method takes a callback function, which is called each time a message is received.

The callback function sends messages to the channel and `Read` method receives messages from this channel.

`Ack` calls the acknowledge method the message was received.

`Teardown` marks all unread messages from the channel the client did not receive them and releases the GCP subscriber client.

#### Configuration
The user can get the authorization data from a JSON file by the following instructions: [Getting started with authentication](https://cloud.google.com/docs/authentication/getting-started).

All fields are required.

| name             | description                        | example                                                                        |
|------------------|------------------------------------|--------------------------------------------------------------------------------|
| `privateKey`     | private key to auth in a client    | -----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG\n-----END PRIVATE KEY-----\n |
| `clientEmail`    | client email to auth in a client   | test_user@conduit-pubsub.iam.gserviceaccount.com                               |
| `projectID`      | project id to auth in a client     | conduit-pubsub                                                                 |
| `subscriptionID` | subscription name to pull messages | conduit-subscription                                                           |