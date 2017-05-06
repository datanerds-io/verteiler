[![Build Status](https://api.travis-ci.org/datanerds-io/verteiler.svg?branch=develop)](https://travis-ci.org/datanerds-io/verteiler)

[![Maven Central](https://img.shields.io/maven-central/v/io.datanerds/verteiler.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.datanerds%22%20a%3A%22verteiler%22)


# Verteiler (fɛɐˈtailɐ)

"Verteiler" is german and translates to _distributor_. When you are using the new'ish `kafka-clients` consumer, you will face the issue that `Consumer.poll(...)` can only be called from [one thread][1]. Verteiler leverages the kafka-client consumer implementation to distribute messages from assigned partitions to `BlockingQueue`s. Each assigned partition will relay its messages to its own internal queue. Additionally, each queue has a consuming thread. Once a message has been processed successfully its offset will be committed asynchronously after `poll(...)` has finished. This way only the offset of processed messages is committed.

# How to use `verteiler`

## `BlockingQueueConsumer`

### Parameters

`String topic`: Name of the topic you want to consume messages from.

`Properties kafkaConfig`: Regular [consumer config][2]. By default `verteiler` will disable auto offset commit and set a client id when not given.

`int queueSize`: Size of the internal queue, which depends on your needs.
                
`java.util.function.Consumer<V> action`: The function which will handle a message. 

### Example
Simple message counter:

```java
Properties props = new Properties();
props.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
...
AtomicInteger messageCounter = new AtomicInteger();
Consumer<String> action = (message) -> messageCounter.incrementAndGet();

BlockingQueueConsumer<String, String> consumer = new BlockingQueueConsumer<>("my_topic", props, 42, action);
consumer.start();
```

For a full example take a look at [BlockingQueueConsumerTest][3].

# Build
`gradle build` will build the project. The integration tests will start embedded zookeeper & kafka instances.


[1]: https://kafka.apache.org/0102/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#multithreaded

[2]: https://kafka.apache.org/0102/javadoc/org/apache/kafka/clients/consumer/ConsumerConfig.html

[3]: https://github.com/datanerds-io/verteiler/blob/develop/it-test/src/test/java/io/datanerds/verteiler/it_test/BlockingQueueConsumerTest.java
