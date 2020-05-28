# Welcome to MaxComput Kafka Connect connector!

## Configuration example
````$xslt
{
  "name": "odps-sink-connector",
  "config": {
    "connector.class": "com.aliyun.odps.kafka.connect.MaxComputeSinkConnector",
    "tasks.max": "3",
    "topics": "your_kafka_topic",
    "endpoint": "http://service.odps.aliyun.com/api",
    "project": "your_maxcompute_project",
    "table": "your_maxcompute_table_to_receive_data",
    "access_id": "your_aliyun_access_id",
    "access_key": "your_aliyun_access_key"
  }
}
````

## Error handling options
- Use [Kafka dead letter queue](https://www.confluent.io/blog/kafka-connect-deep-dive-error-handling-dead-letter-queues/) to handle messages that doesn’t match the specific serialization 
- Use `runtime.error` config to handle messages that an error occurs when processing to MaxCompute, you need to set:
````$xslt
"runtime.error.topic.name":"your topic name to route the message"
"runtime.error.topic.bootstrap.servers": "the bootstrap servers of error topic queue"
````


