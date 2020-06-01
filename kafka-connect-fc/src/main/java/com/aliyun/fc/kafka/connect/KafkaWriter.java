package com.aliyun.fc.kafka.connect;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class KafkaWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaWriter.class);

  private Properties props;
  private String topic;
  private  KafkaProducer producer;


  public KafkaWriter(FunctionComputeSinkConnectorConfig config) {
    props = new Properties();

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(FunctionComputeSinkConnectorConfig.RUNTIME_ERROR_TOPIC_BOOTSTRAP_SERVERS));
    //Kafka消息的序列化方式,这里先默认 String
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    //请求的最长等待时间
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60 * 1000);
    topic = config.getString(FunctionComputeSinkConnectorConfig.RUNTIME_ERROR_TOPIC_NAME);
    producer = new KafkaProducer<String, String>(props);
  }

  public void write(Record message) {
    ProducerRecord<String, String> producerRecord;
    if (message.getTimestamp() == RecordBatch.NO_TIMESTAMP) {
      producerRecord = new ProducerRecord<>(topic, null,
              (String)message.getKey(), (String)message.getValue());
    } else {
      producerRecord = new ProducerRecord<>(topic, null, message.getTimestamp(),
              (String)message.getKey(), (String)message.getValue());
    }

    this.producer.send(producerRecord, (metadata, exception) -> {
      if (exception != null) {
        LOGGER.error("Could not produce message to runtime error queue. topic=" + topic, exception);
      }

      LOGGER.info("Write message to runtime error queue ok: " + metadata.toString());
    });

  }

  public void close() {
    producer.close();
  }

}
