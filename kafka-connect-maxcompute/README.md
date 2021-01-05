# Welcome to MaxComput Kafka Connect connector!

## Configuration example
````$xslt
{
    "name": "your_name",
    "config": {
        "connector.class": "com.aliyun.odps.kafka.connect.MaxComputeSinkConnector",
        "tasks.max": "3",
        "topics": "your_topic",
        "endpoint": "endpoint",
        "project": "project",
        "table": "your_table",
        "account_type": "account type (STS or ALIYUN)",
        "access_id": "access id",
        "access_key": "access key",
        "account_id": "account id for sts",
        "sts.endpoint": "sts endpoint",
        "region_id": "region id for sts",
        "role_name": "role name for sts",
        "client_timeout_ms": "STS Token valid period (ms)",
        "format": "TEXT",
        "mode": "KEY",
        "partition_window_type": "MINUTE"
    }
}
````

## Configuration detail
- name：kafka connector的唯一名称。再次尝试使用相同名称将失败。
- tasks.max：为此connector应创建的最大任务数，必须为大于0的整数。如果connector无法实现此级别的并行，则创建较少的任务。
- topics：用作此connector输入的topic列表。
- endpoint：访问MaxCompute的endpoint。
- project：MaxCompute表所在的project。
- table：要写入的MaxCompute表。
- account_type：MaxCompute鉴权方式，选项为STS或ALIYUN，默认ALIYUN。
- access_id和access_key：若account_type为ALIYUN，则这两项配置为用户的access_id和access_key。否则保持为空即可，但不能不配置这两项。
- account_id、region_id、role_name和client_timeout_ms：生成STS Token所需信息。若account_type为STS，则如实配置。否则可以不配置。
- client_timeout_ms：刷新STS Token的时间间隔，单位为毫秒，默认值为11小时对应的毫秒数。
- sts.endpoint：可选配置，保持默认即可。
- format：消息的格式，详细解释见官方文档，可选值为TEXT、BINARY与CSV，默认TEXT。
- mode：此connector的处理模式，详细解释见官方文档，可选值为：KEY，VALUE，DEFAULT，默认DEFAULT。
- partition_window_type：如何按照系统时间进行数据分区。例如，若配置为MINUTE，则每分钟开始时数据写到一个新的分区。可选值DAY、HOUR、MINUTE，默认HOUR。
