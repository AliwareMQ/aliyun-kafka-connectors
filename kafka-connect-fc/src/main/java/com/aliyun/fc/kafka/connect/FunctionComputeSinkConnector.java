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

import com.aliyun.fc.kafka.connect.utils.Version;
import com.aliyuncs.fc.client.FunctionComputeClient;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Connector class for Function Compute
 */
public class FunctionComputeSinkConnector extends SinkConnector {
  private static final Logger LOG = LoggerFactory.getLogger(FunctionComputeSinkConnector.class);

  private Map<String, String> configProps;
  private FunctionComputeSinkConnectorConfig config;

  public FunctionComputeSinkConnector() {
  }

  @Override
  public void start(Map<String, String> configProps) {
    this.configProps = configProps;
    config = new FunctionComputeSinkConnectorConfig(configProps);

    for (Map.Entry<String, String> entry : configProps.entrySet()) {
      String key = entry.getKey();
      if (!key.equals(FunctionComputeSinkConnectorConfig.ACCESS_ID) && !key.equals(FunctionComputeSinkConnectorConfig.ACCESS_KEY)) {
        LOG.info(entry.getKey() + ": " + entry.getValue());
      }
    }

    LOG.info("Connect to function compute start success!");

  }

  @Override
  public Class<? extends Task> taskClass() {
    return FunctionComputeSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    Map<String, String> taskProps = new HashMap<>(configProps);
    List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
    for (int i = 0; i < maxTasks; ++i) {
      taskConfigs.add(taskProps);
    }
    return taskConfigs;
  }

  @Override
  public void stop() {
    LOG.info("Shutting down function compute sink connector!");
  }

  @Override
  public ConfigDef config() {
    return FunctionComputeSinkConnectorConfig.conf();
  }

  @Override
  public String version() {
    return Version.getVersion();
  }
}
