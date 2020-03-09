package com.aliyun.odps.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.kafka.connect.MaxComputeSinkConnectorConfig;


public class KafkaWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaWriter.class);

  private Properties props;
  private String topic;
  private  KafkaProducer producer;


  public KafkaWriter(MaxComputeSinkConnectorConfig config) {
    props = new Properties();

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(MaxComputeSinkConnectorConfig.RUNTIME_ERROR_TOPIC_BOOTSTRAP_SERVERS));
    //Kafka消息的序列化方式,这里先默认 String
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    //请求的最长等待时间
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30 * 1000);
    topic = config.getString(MaxComputeSinkConnectorConfig.RUNTIME_ERROR_TOPIC_NAME);
    producer = new KafkaProducer<String, String>(props);
  }

  public void write(SinkRecord message) {
    ProducerRecord<String, String> producerRecord;
    if (message.timestamp() == RecordBatch.NO_TIMESTAMP) {
      producerRecord = new ProducerRecord<>(topic, null,
                                                          (String)(message.key()), (String)(message.value()));
    } else {
      producerRecord = new ProducerRecord<>(topic, null, message.timestamp(),
                                                          (String)(message.key()), (String)(message.value()));
    }

    this.producer.send(producerRecord, (metadata, exception) -> {
      if (exception != null) {
        LOGGER.error("Could not produce message to runtime error queue. topic=" + topic, exception);
      }

      LOGGER.info("Write message to runtime error queue ok: " + metadata.toString());
    });

  }

}
