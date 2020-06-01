# Welcome to Function Compute Kafka Connect connector!

## Configuration example
````$xslt
{
  "name": "fc-sink-connector",
  "config": {
    "connector.class": "com.aliyun.fc.kafka.connect.FunctionComputeSinkConnector",
    "tasks.max": "3",
    "topics": "< Required Configuration >",
    "role.name": "< Required Configuration >",
    "region.id": "< Required Configuration >"
    "account.id": "< Required Configuration >"
    "service.name": "< Required Configuration >"
    "function.name": "< Required Configuration >"
    "invocation.type": "sync or async"
    "batch.size": "50"
    "runtime.error.topic.bootstrap.servers": "< Required Configuration >",
    "runtime.error.topic.name": "< Required Configuration >"
  }
}
````