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
import com.aliyun.odps.account.AliyunAccount;


public class MaxComputeSinkConnector extends SinkConnector {
  private static final Logger LOGGER = LoggerFactory.getLogger(MaxComputeSinkConnector.class);
  private MaxComputeSinkConnectorConfig config;
  private Odps odps;

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

    AliyunAccount account = new AliyunAccount(
        config.getString(MaxComputeSinkConnectorConfig.ACCESS_ID),
        config.getString(MaxComputeSinkConnectorConfig.ACCESS_KEY));

    odps = new Odps(account);
    odps.setDefaultProject(config.getString(MaxComputeSinkConnectorConfig.MAXCOMPUTE_PROJECT));
    odps.setEndpoint(config.getString(MaxComputeSinkConnectorConfig.MAXCOMPUTE_ENDPOINT));

    try {
      odps.projects().exists(config.getString(MaxComputeSinkConnectorConfig.MAXCOMPUTE_PROJECT));
      odps.tables().exists(config.getString(MaxComputeSinkConnectorConfig.MAXCOMPUTE_TABLE));
    } catch (OdpsException e) {
      throw new IllegalArgumentException("Cannot find configured MaxCompute project or table");
    }

    LOGGER.info("Connect to MaxCompute successfully!");
  }

  @Override
  public Class<? extends Task> taskClass() {
    return MaxComputeSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> taskConfigs = new LinkedList<>();
    for (int i = 0; i < maxTasks; i++) {
      Map<String, String> taskConfig = new HashMap<>();
      taskConfig.put(MaxComputeSinkConnectorConfig.ACCESS_ID,
                     config.getString(MaxComputeSinkConnectorConfig.ACCESS_ID));
      taskConfig.put(MaxComputeSinkConnectorConfig.ACCESS_KEY,
                     config.getString(MaxComputeSinkConnectorConfig.ACCESS_KEY));
      taskConfig.put(MaxComputeSinkConnectorConfig.MAXCOMPUTE_PROJECT,
                     config.getString(MaxComputeSinkConnectorConfig.MAXCOMPUTE_PROJECT));
      taskConfig.put(MaxComputeSinkConnectorConfig.MAXCOMPUTE_ENDPOINT,
                     config.getString(MaxComputeSinkConnectorConfig.MAXCOMPUTE_ENDPOINT));
      taskConfig.put(MaxComputeSinkConnectorConfig.MAXCOMPUTE_TABLE,
                     config.getString(MaxComputeSinkConnectorConfig.MAXCOMPUTE_TABLE));

      ((LinkedList<Map<String, String>>) taskConfigs).push(taskConfig);
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
