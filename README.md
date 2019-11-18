## Implementing kafka consumers using sarama-cluster

There aren’t a huge number of viable options when it comes to implementing a Kafka consumer in Go. This tutorial focuses on [sarama-cluster](https://github.com/bsm/sarama-cluster), a balanced consumer implementation built on top the existing [sarama](https://github.com/Shopify/sarama) client library by **[Shopify]()**.

```go
consumer, err := cluster.NewConsumer(
  []string{"broker-address-1", "broker-address-2"},
  "group-id",
  []string{"topic-1", "topic-2", "topic-3"},
  kafkaConfig)

if err != nil {
  panic(err)
}
```


The sarama-cluster library allows you to specify a consumer mode within the config. It’s important to understand the difference as your implementation will differ based on what you’ve chosen. This can be modified via the config.Group.Mode struct field and has two options. These are:

- **ConsumerModeMultiplex** - By default, messages and errors from the subscribed *topics* and *partitions* are all multiplexed and made available through the consumer’s ***Messages()*** and ***Errors()*** channels.
- **ConsumerModePartitions** - Users who require low-level access can enable ***ConsumerModePartitions*** where individual partitions are exposed on the **Partitions()** channel. Messages and errors must then be consumed on the partitions themselves.

When using ConsumerModeMultiplex, all messages come from a single channel exposed via the Messages() method. Reading these messages looks like this:

```go
// The loop will iterate each time a message is written to the underlying channel
for msg := range consumer.Messages() {
  // Now we can access the individual fields of the message and react
  // based on msg.Topic
  switch msg.Topic {
    case "topic-1":
      handleTopic1(msg.Value)
      break;
    // ...
  }
}
```

Low-level access and react to partition changes, use ***ConsumerModePartitions*** mode, which provides the *individual partitions* via ***consumer.Partitions()*** method. This exposes an underlying channel that partitions are written to when consumer group *rebalances*:

```go
// Every time the consumer is balanced, we'll get a new partition to read from
for partition := range consumer.Partitions() {
  // From here, we know exactly which topic we're consuming via partition.Topic(). So won't need any
  // branching logic based on the topic.
  for msg := range consumer.Messages() {
    // Now we can access the individual fields of the message
    handleTopic1(msg.Value)   
  }
}
```

The ***ConsumerModePartitions*** way of doing things will require you to code more oversight into your consumer. For one, you’re going to need to gracefully handle the situation where the partition closes in a rebalance situation. These will occur when adding new consumers to the group. You’re also going to need to manually call the ***partition.Close()*** method when you’re done consuming.



## Handling errors & rebalances

Should you **add** more consumers to the group, the ***existing ones will experience a rebalance***. This is where the assignment of partitions to each consumer changes for an optimal spread across consumers. The consumer instance we’ve created already exposes a ***Notifications()*** channel from which we can log/react to these changes.

```go
for notification := range consumer.Notifications() {
    // The type of notification we've received, will be
    // rebalance start, rebalance ok or error
    fmt.Println(notification.Type)

    // The topic/partitions that are currently read by the consumer
    fmt.Println(notification.Current)

    // The topic/partitions that were claimed in the last rebalance
    fmt.Println(notification.Claimed)

    // The topic/partitions that were released in the last rebalance
    fmt.Println(notification.Released)
}
```

Errors are just as easy to read and are made available via the consumer.Errors() channel. They return a standard error implementation.

```go
for err := range consumer.Errors() {
    // React to the error
}
```

In order to enable the reading of notification and errors, we need to make some small changes to our configuration like so:

```go
config.Consumer.Return.Errors = true
config.Group.Return.Notifications = true
```


## Committing offsets

We’re telling Kafka that we have finished processing a message and we do not want to consume it again. If you commit offsets ***too early** you may loose the opportunity to ***re-consume*** the event if something goes wrong.

```go
// The loop will iterate each time a message is written to the underlying channel
for msg := range consumer.Messages() {
  // Now we can access the individual fields of the message and react
  // based on msg.Topic
  switch msg.Topic {
    case "topic-1":
      // Do everything we need for this topic
      handleTopic1(msg.Value)

      // Mark the message as processed. The sarama-cluster library will
      // automatically commit these.
      // You can manually commit the offsets using consumer.CommitOffsets()
      consumer.MarkOffset(msg)
      break;
      // ...
  }
}
```

## References:
- [Implementing kafka consumers using sarama-cluster](https://dev.to/davidsbond/golang-implementing-kafka-consumers-using-sarama-cluster-4fko)
- [Writing and Testing an Event Sourcing Microservice with Kafka and Go](https://semaphoreci.com/community/tutorials/writing-and-testing-an-event-sourcing-microservice-with-kafka-and-go)
- [Sarama - FAQs](https://github.com/Shopify/sarama/wiki/Frequently-Asked-Questions)
- [Impementing CQRS using Kafka and Sarama Library in Golang](https://medium.com/@Oskarr3/implementing-cqrs-using-kafka-and-sarama-library-in-golang-da7efa3b77fe)
- [Simple messaging framework using Go TCP server and Kafka](https://blog.gopheracademy.com/advent-2017/messaging-framework/)
