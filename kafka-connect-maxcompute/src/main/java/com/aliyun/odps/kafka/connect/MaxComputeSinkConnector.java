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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.kafka.connect.utils.OdpsUtils;


public class MaxComputeSinkConnector extends SinkConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(MaxComputeSinkConnector.class);
  private MaxComputeSinkConnectorConfig config;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    config = new MaxComputeSinkConnectorConfig(map);

    LOGGER.info("Starting MaxCompute sink connector");
    for (Entry<String, String> entry : map.entrySet()) {
      LOGGER.info(entry.getKey() + ": " + entry.getValue());
    }

    Odps odps = OdpsUtils.getOdps(config);

    try {
      odps.projects().exists(config.getString(MaxComputeSinkConnectorConfig.MAXCOMPUTE_PROJECT));
      odps.tables().exists(config.getString(MaxComputeSinkConnectorConfig.MAXCOMPUTE_TABLE));
    } catch (OdpsException e) {
      throw new IllegalArgumentException("Cannot find configured MaxCompute project or table");
    }

    // TODO: validate table schema

    LOGGER.info("Connect to MaxCompute successfully!");
  }

  @Override
  public Class<? extends Task> taskClass() {
    return MaxComputeSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    LinkedList<Map<String, String>> taskConfigs = new LinkedList<>();
    for (int i = 0; i < maxTasks; i++) {
      Map<String, String> taskConfig = new HashMap<>();
      taskConfig.put(MaxComputeSinkConnectorConfig.ACCESS_ID,
                     config.getString(MaxComputeSinkConnectorConfig.ACCESS_ID));
      taskConfig.put(MaxComputeSinkConnectorConfig.ACCESS_KEY,
                     config.getString(MaxComputeSinkConnectorConfig.ACCESS_KEY));
      taskConfig.put(MaxComputeSinkConnectorConfig.ACCOUNT_ID,
                     config.getString(MaxComputeSinkConnectorConfig.ACCOUNT_ID));
      taskConfig.put(MaxComputeSinkConnectorConfig.REGION_ID,
                     config.getString(MaxComputeSinkConnectorConfig.REGION_ID));
      taskConfig.put(MaxComputeSinkConnectorConfig.STS_ENDPOINT,
                     config.getString(MaxComputeSinkConnectorConfig.STS_ENDPOINT));
      taskConfig.put(MaxComputeSinkConnectorConfig.ROLE_NAME,
                     config.getString(MaxComputeSinkConnectorConfig.ROLE_NAME));
      taskConfig.put(MaxComputeSinkConnectorConfig.ACCOUNT_TYPE,
                     config.getString(MaxComputeSinkConnectorConfig.ACCOUNT_TYPE));
      taskConfig.put(MaxComputeSinkConnectorConfig.CLIENT_TIMEOUT_MS,
                     String.valueOf(config.getLong(MaxComputeSinkConnectorConfig.CLIENT_TIMEOUT_MS)));
      taskConfig.put(MaxComputeSinkConnectorConfig.MAXCOMPUTE_PROJECT,
                     config.getString(MaxComputeSinkConnectorConfig.MAXCOMPUTE_PROJECT));
      taskConfig.put(MaxComputeSinkConnectorConfig.MAXCOMPUTE_ENDPOINT,
                     config.getString(MaxComputeSinkConnectorConfig.MAXCOMPUTE_ENDPOINT));
      taskConfig.put(MaxComputeSinkConnectorConfig.MAXCOMPUTE_TABLE,
                     config.getString(MaxComputeSinkConnectorConfig.MAXCOMPUTE_TABLE));
      taskConfig.put(MaxComputeSinkConnectorConfig.RUNTIME_ERROR_TOPIC_BOOTSTRAP_SERVERS, config
          .getString(MaxComputeSinkConnectorConfig.RUNTIME_ERROR_TOPIC_BOOTSTRAP_SERVERS));
      taskConfig.put(MaxComputeSinkConnectorConfig.RUNTIME_ERROR_TOPIC_NAME,
                     config.getString(MaxComputeSinkConnectorConfig.RUNTIME_ERROR_TOPIC_NAME));
      taskConfig.put(MaxComputeSinkConnectorConfig.FORMAT,
                     config.getString(MaxComputeSinkConnectorConfig.FORMAT));
      taskConfig.put(MaxComputeSinkConnectorConfig.MODE,
                     config.getString(MaxComputeSinkConnectorConfig.MODE));
      taskConfig.put(MaxComputeSinkConnectorConfig.PARTITION_WINDOW_TYPE,
                     config.getString(MaxComputeSinkConnectorConfig.PARTITION_WINDOW_TYPE));
      taskConfig.put(MaxComputeSinkConnectorConfig.TIME_ZONE,
                     config.getString(MaxComputeSinkConnectorConfig.TIME_ZONE));

      taskConfigs.push(taskConfig);
    }

    return taskConfigs;
  }

  @Override
  public void stop() {
    // do nothing
  }

  @Override
  public ConfigDef config() {
    return MaxComputeSinkConnectorConfig.conf();
  }
}
