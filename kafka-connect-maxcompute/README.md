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
