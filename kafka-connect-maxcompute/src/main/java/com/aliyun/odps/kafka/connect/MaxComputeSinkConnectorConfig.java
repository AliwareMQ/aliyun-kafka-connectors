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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;


public class MaxComputeSinkConnectorConfig extends AbstractConfig {

  public static final String MAXCOMPUTE_ENDPOINT = "endpoint";
  public static final String MAXCOMPUTE_PROJECT = "project";
  public static final String MAXCOMPUTE_TABLE = "table";
  public static final String ACCESS_ID = "access_id";
  public static final String ACCESS_KEY = "access_key";

  public MaxComputeSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);
  }

  public MaxComputeSinkConnectorConfig(Map<String, String> parsedConfig) {
    this(conf(), parsedConfig);
  }

  public static ConfigDef conf() {
    ConfigDef configDef = new ConfigDef();
    configDef.define(MAXCOMPUTE_ENDPOINT,
                     Type.STRING,
                     Importance.HIGH,
                     "MaxCompute endpoint")
             .define(MAXCOMPUTE_PROJECT,
                     Type.STRING,
                     Importance.HIGH,
                     "MaxCompute project")
             .define(MAXCOMPUTE_TABLE,
                     Type.STRING,
                     Importance.HIGH,
                     "MaxCompute table")
             .define(ACCESS_ID,
                     Type.STRING,
                     Importance.HIGH,
                     "Aliyun access ID")
             .define(ACCESS_KEY,
                     Type.STRING,
                     Importance.HIGH,
                     "Aliyun access key");
    return configDef;
  }
}
