/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package com.aliyun.odps.kafka.connect;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.kafka.connect.converter.RecordConverterBuilder;
import com.aliyun.odps.kafka.connect.converter.RecordConverterBuilder.ConverterType;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;


public class MaxComputeSinkTask extends SinkTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(MaxComputeSinkTask.class);

  private Odps odps;
  private TableTunnel tunnel;
  private String project;
  private String table;
  private Map<TopicPartition, MaxComputeSinkWriter> writers = new ConcurrentHashMap<>();

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    LOGGER.info("Thread(" + Thread.currentThread().getId() + ") Enter OPEN");
    for (TopicPartition partition : partitions) {
      LOGGER.info("Thread(" + Thread.currentThread().getId() + ") OPEN (topic: " +
                      partition.topic() + ", partition: " + partition.partition() + ")");
    }

    for (TopicPartition partition : partitions) {
      resumeCheckPoint(partition);

      // TODO: support more converters
      RecordConverterBuilder converterBuilder = new RecordConverterBuilder();
      converterBuilder.type(ConverterType.DEFAULT);
      try {
        MaxComputeSinkWriter writer = new MaxComputeSinkWriter(tunnel,
                                                               project,
                                                               table,
                                                               converterBuilder.build(),
                                                               64);
        writers.put(partition, writer);
        LOGGER.info("Thread(" + Thread.currentThread().getId() +
                        ") Initialize writer successfully for (topic: " + partition.topic() +
                        ", partition: " + partition.partition() + ")");
      } catch (TunnelException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Start from last committed offset
   */
  private void resumeCheckPoint(TopicPartition partition) {
    StringBuilder queryBuilder = new StringBuilder();
    queryBuilder.append("SELECT MAX(offset) as offset ")
                .append("FROM ").append(table).append(" ")
                .append("WHERE topic=\"").append(partition.topic()).append("\" ")
                .append("AND partition=").append(partition.partition()).append(";");

    try {
      Instance findLastCommittedOffset = SQLTask.run(odps, queryBuilder.toString());
      findLastCommittedOffset.waitForSuccess();
      ResultSet res = SQLTask.getResultSet(findLastCommittedOffset);
      Long lastCommittedOffset = res.next().getBigint("offset");
      LOGGER.info("Thread(" + Thread.currentThread().getId() +
                      ") Last committed offset for (topic: " + partition.topic() + ", partition: "
                      + partition.partition() + "): " + lastCommittedOffset);
      if (lastCommittedOffset != null) {
        // Offset should be reset to the last committed plus one, otherwise the last committed
        // record will be duplicated
        context.offset(partition, lastCommittedOffset + 1);
      }
    } catch (OdpsException | IOException e) {
      LOGGER.error("Thread(" + Thread.currentThread().getId() + ") Resume from checkpoint failed");
      throw new RuntimeException(e);
    }
  }

  @Override
  public void start(Map<String, String> map) {
    LOGGER.info("Thread(" + Thread.currentThread().getId() + ") Enter START");

    MaxComputeSinkConnectorConfig config = new MaxComputeSinkConnectorConfig(map);
    String endpoint = config.getString(MaxComputeSinkConnectorConfig.MAXCOMPUTE_ENDPOINT);
    String accessId = config.getString(MaxComputeSinkConnectorConfig.ACCESS_ID);
    String accessKey = config.getString(MaxComputeSinkConnectorConfig.ACCESS_KEY);
    project = config.getString(MaxComputeSinkConnectorConfig.MAXCOMPUTE_PROJECT);
    table = config.getString(MaxComputeSinkConnectorConfig.MAXCOMPUTE_TABLE);

    LOGGER.info("Thread(" + Thread.currentThread().getId() + ") Starting MaxCompute sink task");

    AliyunAccount account = new AliyunAccount(accessId, accessKey);

    odps = new Odps(account);
    odps.setDefaultProject(project);
    odps.setEndpoint(endpoint);
    tunnel = new TableTunnel(odps);

    LOGGER.info("Thread(" + Thread.currentThread().getId() + ") Start MaxCompute sink task done");
  }

  @Override
  public void put(Collection<SinkRecord> collection) {
    LOGGER.info("Thread(" + Thread.currentThread().getId() + ") Enter PUT");
    for (SinkRecord record : collection) {
      LOGGER.info("Thread(" + Thread.currentThread().getId() + ") PUT " + record.toString());
    }

    long time = System.currentTimeMillis();

    for (SinkRecord r : collection) {
      TopicPartition partition = new TopicPartition(r.topic(), r.kafkaPartition());
      MaxComputeSinkWriter writer = writers.get(partition);
      try {
        writer.write(r, time);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    LOGGER.info("Thread(" + Thread.currentThread().getId() + ") Enter FLUSH");
    for (Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
      LOGGER.info("Thread(" + Thread.currentThread().getId() + ") FLUSH "
                      + "(topic: " + entry.getKey().topic() +
                      ", partition: " + entry.getKey().partition() + ")");
    }

    for (Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
      TopicPartition partition = entry.getKey();
      MaxComputeSinkWriter writer = writers.get(partition);

      // If the writer is not initialized, there is nothing to commit
      if (writer == null) {
        continue;
      }

      try {
        writer.close();
      } catch (IOException e) {
        LOGGER.error(e.getMessage());
        resetOffset(partition);
      }

      // Create new writer for this topic
      RecordConverterBuilder converterBuilder = new RecordConverterBuilder();
      converterBuilder.type(ConverterType.DEFAULT);
      try {
        MaxComputeSinkWriter newWriter = new MaxComputeSinkWriter(tunnel,
                                                                  project,
                                                                  table,
                                                                  converterBuilder.build(),
                                                                  64);
        writers.put(partition, newWriter);
      } catch (TunnelException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    LOGGER.info("Thread(" + Thread.currentThread().getId() + ") Enter CLOSE");
    for (TopicPartition partition : partitions) {
      LOGGER.info("Thread(" + Thread.currentThread().getId() +
                      ") CLOSE (topic: " + partition.topic() +
                      ", partition: " + partition.partition() + ")");
    }

    for (TopicPartition partition : partitions) {
      MaxComputeSinkWriter writer = writers.get(partition);

      // If the writer is not initialized, there is nothing to commit
      if (writer == null) {
        continue;
      }

      try {
        writer.close();
      } catch (IOException e) {
        LOGGER.error(e.getMessage());
        resetOffset(partition);
      }

      writers.remove(partition);
    }
  }

  @Override
  public void stop() {
    LOGGER.info("Thread(" + Thread.currentThread().getId() + ") Enter STOP");

    for (Entry<TopicPartition, MaxComputeSinkWriter> entry : writers.entrySet()) {
      try {
        entry.getValue().close();
      } catch (IOException e) {
        LOGGER.error(e.getMessage());
        resetOffset(entry.getKey());
      }
    }

    writers.clear();
  }

  /**
   * Reset the offset of given partition.
   */
  private void resetOffset(TopicPartition partition) {
    MaxComputeSinkWriter writer = writers.get(partition);

    LOGGER.info("Thread(" + Thread.currentThread().getId() + ") Reset offset to " +
                    writer.getMinOffset() + ", topic: " + partition.topic() +
                    ", partition: " + partition.partition());
    // Reset offset
    context.offset(partition, writer.getMinOffset());
  }
}
