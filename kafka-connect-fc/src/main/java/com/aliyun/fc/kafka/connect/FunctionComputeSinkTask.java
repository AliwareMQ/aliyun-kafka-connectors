/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.fc.kafka.connect;

import com.aliyun.fc.kafka.connect.enums.InvocationTypeEnum;
import com.aliyun.fc.kafka.connect.obj.StsUserBo;
import com.aliyun.fc.kafka.connect.service.StsService;
import com.aliyun.fc.kafka.connect.utils.Version;
import com.aliyuncs.fc.client.FunctionComputeClient;
import com.aliyuncs.fc.config.Config;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class FunctionComputeSinkTask extends SinkTask {
  private static final Logger LOG = LoggerFactory.getLogger(FunctionComputeSinkTask.class);

  private FunctionComputeSinkConnectorConfig config;
  String regionId;
  String accountId;
  String accessId;
  String accessKey;
  String roleName;
  String endPoint;
  String service;
  String function;
  String qualifier;
  String invocationType;
  int batchSize;
  int maxRequestSize;
  long clientTimeOutMs;
  KafkaWriter runtimeErrorWriter;
  FunctionComputeSinkWriter functionComputeSinkWriter;
  long clientCreateLastTime;

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> properties) {
    LOG.info("Thread(" + Thread.currentThread().getId() + ") Enter START");

    config = new FunctionComputeSinkConnectorConfig(properties);
    regionId = config.getString(FunctionComputeSinkConnectorConfig.REGION);
    accountId = config.getString(FunctionComputeSinkConnectorConfig.ACCOUNT_ID);
    accessId = config.getString(FunctionComputeSinkConnectorConfig.ACCESS_ID);
    accessKey = config.getString(FunctionComputeSinkConnectorConfig.ACCESS_KEY);
    roleName = config.getString(FunctionComputeSinkConnectorConfig.ROLE_NAME);
    endPoint = config.getString(FunctionComputeSinkConnectorConfig.ENDPOINT);
    service = config.getString(FunctionComputeSinkConnectorConfig.SERVICE);
    function = config.getString(FunctionComputeSinkConnectorConfig.FUNCTION);
    qualifier = config.getString(FunctionComputeSinkConnectorConfig.QUALIFIER);
    invocationType = config.getString(FunctionComputeSinkConnectorConfig.INVOCATION_TYPE);
    batchSize = config.getInt(FunctionComputeSinkConnectorConfig.BATCH_SIZE);
    clientTimeOutMs = config.getLong(FunctionComputeSinkConnectorConfig.CLIENT_TIME_OUT_MS);
    if (invocationType.toLowerCase().equals(InvocationTypeEnum.SYNC.getValue().toLowerCase())) {
      maxRequestSize = config.getInt(FunctionComputeSinkConnectorConfig.SYNC_MAX_REQUEST_SIZE);
    } else if (invocationType.toLowerCase().equals(InvocationTypeEnum.ASYNC.getValue().toLowerCase())) {
      maxRequestSize = config.getInt(FunctionComputeSinkConnectorConfig.ASYNC_MAX_REQUEST_SIZE);
    } else {
      throw new RuntimeException("Invalid invocation type:" + invocationType);
    }
    initOrRebuildClient();
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    LOG.info("Thread(" + Thread.currentThread().getId() + ") Enter OPEN");
    for (TopicPartition partition : partitions) {
      LOG.info("Thread(" + Thread.currentThread().getId() + ") OPEN (topic: " +
              partition.topic() + ", partition: " + partition.partition() + ")");
    }
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      return;
    }

    if (System.currentTimeMillis() - clientCreateLastTime > clientTimeOutMs) {
      initOrRebuildClient();
    }

    functionComputeSinkWriter.write(records);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Read {} records from Kafka", records.size());
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    LOG.info("Thread(" + Thread.currentThread().getId() + ") Enter Close");
    for (TopicPartition partition : partitions) {
      LOG.info("Thread(" + Thread.currentThread().getId() + ") CLOSE (topic: " +
              partition.topic() + ", partition: " + partition.partition() + ")");
    }
    runtimeErrorWriter.close();
  }

  @Override
  public void stop() {
    LOG.info("Thread(" + Thread.currentThread().getId() + ") Enter Stop");
    functionComputeSinkWriter.close();
  }

  public void initOrRebuildClient() {
    StsUserBo stsUserBo = new StsService().getAssumeRole(accountId, regionId, accessId, accessKey, roleName);
    clientCreateLastTime = System.currentTimeMillis();
    FunctionComputeClient fcClient = new FunctionComputeClient(
            new Config(regionId, stsUserBo.getOwnId(),
                    stsUserBo.getAk(), stsUserBo.getSk(), stsUserBo.getToken(),
                    false)
    );
    fcClient.setEndpoint(endPoint);

    if (runtimeErrorWriter == null) {
      runtimeErrorWriter = new KafkaWriter(config);
    }
    functionComputeSinkWriter = new FunctionComputeSinkWriter(fcClient, service, function, qualifier, InvocationTypeEnum.get(invocationType.toUpperCase()), batchSize, maxRequestSize, runtimeErrorWriter);
  }
}
