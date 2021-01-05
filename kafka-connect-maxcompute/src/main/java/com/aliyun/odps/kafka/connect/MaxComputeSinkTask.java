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
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.kafka.KafkaWriter;
import com.aliyun.odps.kafka.connect.converter.RecordConverterBuilder;
import com.aliyun.odps.kafka.connect.utils.OdpsUtils;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoParser;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class MaxComputeSinkTask extends SinkTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(MaxComputeSinkTask.class);

  private Odps odps;
  private String project;
  private String table;
  private RecordConverterBuilder converterBuilder;
  private PartitionWindowType partitionWindowType;
  private TimeZone tz;
  private Map<TopicPartition, MaxComputeSinkWriter> writers = new ConcurrentHashMap<>();
  private KafkaWriter runtimeErrorWriter = null;

  /*
    Performance metrics
   */
  private long totalBytesSentByClosedWriters = 0;
  private long startTimestamp;

  /**
   * For Account
   */
  private MaxComputeSinkConnectorConfig config;
  private long odpsCreateLastTime;
  private long timeout;
  private String accountType;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    LOGGER.debug("Thread(" + Thread.currentThread().getId() + ") Enter OPEN");
    for (TopicPartition partition : partitions) {
      LOGGER.info("Thread(" + Thread.currentThread().getId() + ") OPEN (topic: " +
                  partition.topic() + ", partition: " + partition.partition() + ")");
    }

    initOrRebuildOdps();

    for (TopicPartition partition : partitions) {
      // TODO: Consider a way to resume when running in key or value mode
//      resumeCheckPoint(partition);

      MaxComputeSinkWriter writer = new MaxComputeSinkWriter(
          odps,
          project,
          table,
          converterBuilder.build(),
          64,
          partitionWindowType,
          tz);
      writers.put(partition, writer);
      LOGGER.info("Thread(" + Thread.currentThread().getId() +
                  ") Initialize writer successfully for (topic: " + partition.topic() +
                  ", partition: " + partition.partition() + ")");
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
      LOGGER.error("Thread(" + Thread.currentThread().getId() + ") Resume from checkpoint failed", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void start(Map<String, String> map) {
    LOGGER.info("Thread(" + Thread.currentThread().getId() + ") Enter START");

    startTimestamp = System.currentTimeMillis();

    config = new MaxComputeSinkConnectorConfig(map);
    accountType = config.getString(MaxComputeSinkConnectorConfig.ACCOUNT_TYPE);
    timeout = config.getLong(MaxComputeSinkConnectorConfig.CLIENT_TIMEOUT_MS);

    String endpoint = config.getString(MaxComputeSinkConnectorConfig.MAXCOMPUTE_ENDPOINT);
    project = config.getString(MaxComputeSinkConnectorConfig.MAXCOMPUTE_PROJECT);
    table = config.getString(MaxComputeSinkConnectorConfig.MAXCOMPUTE_TABLE);

    // Init odps
    odps = OdpsUtils.getOdps(config);
    odpsCreateLastTime = System.currentTimeMillis();
    odps.setDefaultProject(project);
    odps.setEndpoint(endpoint);

    // Init converter builder
    RecordConverterBuilder.Format format = RecordConverterBuilder.Format.valueOf(
        config.getString(MaxComputeSinkConnectorConfig.FORMAT));
    RecordConverterBuilder.Mode mode = RecordConverterBuilder.Mode.valueOf(
        config.getString(MaxComputeSinkConnectorConfig.MODE));
    converterBuilder = new RecordConverterBuilder();
    converterBuilder.format(format).mode(mode);
    converterBuilder.schema(odps.tables().get(table).getSchema());

    // Parse partition window size
    partitionWindowType = PartitionWindowType.valueOf(
        config.getString(MaxComputeSinkConnectorConfig.PARTITION_WINDOW_TYPE));
    // Parse time zone
    tz = TimeZone.getTimeZone(config.getString(MaxComputeSinkConnectorConfig.TIME_ZONE));

    if (!StringUtils.isNullOrEmpty(
        config.getString(MaxComputeSinkConnectorConfig.RUNTIME_ERROR_TOPIC_NAME))
        && !StringUtils.isNullOrEmpty(
        config.getString(MaxComputeSinkConnectorConfig.RUNTIME_ERROR_TOPIC_BOOTSTRAP_SERVERS))) {

      runtimeErrorWriter = new KafkaWriter(config);
      LOGGER.info(
          "Thread(" + Thread.currentThread().getId() + ") new runtime error kafka writer done");
    }

    LOGGER.info("Thread(" + Thread.currentThread().getId() + ") Start MaxCompute sink task done");
  }

  @Override
  public void put(Collection<SinkRecord> collection) {
    LOGGER.debug("Thread(" + Thread.currentThread().getId() + ") Enter PUT");

    // Epoch second
    long time = System.currentTimeMillis() / 1000;

    boolean rebuilt = initOrRebuildOdps();
    for (SinkRecord r : collection) {
      TopicPartition partition = new TopicPartition(r.topic(), r.kafkaPartition());
      MaxComputeSinkWriter writer = writers.get(partition);
      try {
        if (rebuilt) {
          writer.refresh(odps);
        }

        writer.write(r, time);
      } catch (IOException e) {
        reportRuntimeError(r, e);
      }
    }
  }

  private void reportRuntimeError(SinkRecord record, IOException e) {
    if (runtimeErrorWriter != null) {
      LOGGER.info("Thread(" + Thread.currentThread().getId() + ") skip runtime error", e);

      runtimeErrorWriter.write(record);
    } else {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Thread(" + Thread.currentThread().getId() + ") Enter FLUSH");
      for (Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
        LOGGER.debug("Thread(" + Thread.currentThread().getId() + ") FLUSH "
                         + "(topic: " + entry.getKey().topic() +
                         ", partition: " + entry.getKey().partition() + ")");
      }
    }

    boolean rebuilt = initOrRebuildOdps();
    for (Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
      TopicPartition partition = entry.getKey();
      MaxComputeSinkWriter writer = writers.get(partition);

      // If the writer is not initialized, there is nothing to commit
      if (writer == null) {
        continue;
      }

      if (rebuilt) {
        writer.refresh(odps);
      }

      // Close writer
      try {
        writer.close();
      } catch (IOException e) {
        LOGGER.error(e.getMessage(), e);
        resetOffset(partition);
      }

      // Update bytes sent
      totalBytesSentByClosedWriters += writer.getTotalBytes();

      // Create new writer
      MaxComputeSinkWriter newWriter = new MaxComputeSinkWriter(
          odps,
          project,
          table,
          converterBuilder.build(),
          64,
          partitionWindowType,
          tz);
      writers.put(partition, newWriter);
    }

    LOGGER.debug("Thread(" + Thread.currentThread().getId() + ") Total bytes sent: " +
                totalBytesSentByClosedWriters +
                ", elapsed time: " + ((System.currentTimeMillis()) - startTimestamp));
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    LOGGER.debug("Thread(" + Thread.currentThread().getId() + ") Enter CLOSE");
    for (TopicPartition partition : partitions) {
      LOGGER.debug("Thread(" + Thread.currentThread().getId() +
                  ") CLOSE (topic: " + partition.topic() +
                  ", partition: " + partition.partition() + ")");
    }

    boolean rebuilt = initOrRebuildOdps();
    for (TopicPartition partition : partitions) {
      MaxComputeSinkWriter writer = writers.get(partition);

      // If the writer is not initialized, there is nothing to commit
      if (writer == null) {
        continue;
      }

      // refresh writer's session
      if (rebuilt) {
        writer.refresh(odps);
      }

      try {
        writer.close();
      } catch (IOException e) {
        LOGGER.error(e.getMessage(), e);
        resetOffset(partition);
      }

      writers.remove(partition);
      totalBytesSentByClosedWriters += writer.getTotalBytes();
    }

    LOGGER.info("Thread(" + Thread.currentThread().getId() + ") Total bytes sent: " +
                totalBytesSentByClosedWriters +
                ", elapsed time: " + ((System.currentTimeMillis()) - startTimestamp));
  }

  @Override
  public void stop() {
    LOGGER.info("Thread(" + Thread.currentThread().getId() + ") Enter STOP");

    for (Entry<TopicPartition, MaxComputeSinkWriter> entry : writers.entrySet()) {
      try {
        entry.getValue().close();
      } catch (IOException e) {
        LOGGER.error(e.getMessage(), e);
        resetOffset(entry.getKey());
      }
    }

    writers.values().forEach(w -> totalBytesSentByClosedWriters += w.getTotalBytes());
    writers.clear();

    LOGGER.info("Thread(" + Thread.currentThread().getId() + ") Total bytes sent: " +
                totalBytesSentByClosedWriters +
                ", elapsed time: " + ((System.currentTimeMillis()) - startTimestamp));
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

  protected static TableSchema parseSchema(String json) {
    TableSchema schema = new TableSchema();
    try {
      JsonObject tree = new JsonParser().parse(json).getAsJsonObject();

      if (tree.has("columns") && tree.get("columns") != null) {
        JsonArray columnsNode = tree.get("columns").getAsJsonArray();
        for (int i = 0; i < columnsNode.size(); ++i) {
          JsonObject n = columnsNode.get(i).getAsJsonObject();
          schema.addColumn(parseColumn(n));
        }
      }

      if (tree.has("partitionKeys") && tree.get("partitionKeys") != null) {
        JsonArray columnsNode = tree.get("partitionKeys").getAsJsonArray();
        for (int i = 0; i < columnsNode.size(); ++i) {
          JsonObject n = columnsNode.get(i).getAsJsonObject();
          schema.addPartitionColumn(parseColumn(n));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }

    return schema;
  }

  private static Column parseColumn(JsonObject node) {
    String name = node.has("name") ? node.get("name").getAsString() : null;

    if (name == null) {
      throw new IllegalArgumentException("Invalid schema, column name cannot be null");
    }

    String typeString = node.has("type") ? node.get("type").getAsString().toUpperCase() : null;

    if (typeString == null) {
      throw new IllegalArgumentException("Invalid schema, column type cannot be null");
    }

    TypeInfo typeInfo = TypeInfoParser.getTypeInfoFromTypeString(typeString);

    return new Column(name, typeInfo, null, null, null);
  }

  private boolean initOrRebuildOdps() {
    LOGGER.debug("Enter initOrRebuildOdps!");

    // Exit fast
    if (!Account.AccountProvider.STS.name().equals(accountType)) {
      return false;
    }

    boolean rebuilt = false;
    long current = System.currentTimeMillis();
    if (current - odpsCreateLastTime > timeout) {
      LOGGER.info("STS AK timed out. Last: {}, current: {}", odpsCreateLastTime, current);
      this.odps = OdpsUtils.getOdps(config);
      odpsCreateLastTime = current;
      rebuilt = true;
      LOGGER.info("Account refreshed. Creation time: {}", current);
    }

    return rebuilt;
  }
}
