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

import java.util.Map;
import java.util.TimeZone;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import com.aliyun.odps.account.Account;


public class MaxComputeSinkConnectorConfig extends AbstractConfig {

  public static final String MAXCOMPUTE_ENDPOINT = "endpoint";
  public static final String MAXCOMPUTE_PROJECT = "project";
  public static final String MAXCOMPUTE_TABLE = "table";
  public static final String ACCESS_ID = "access_id";
  public static final String ACCESS_KEY = "access_key";
  public static final String ACCOUNT_ID = "account_id";
  public static final String REGION_ID = "region_id";
  public static final String STS_ENDPOINT = "sts.endpoint";
  public static final String ROLE_NAME = "role_name";
  public static final String ACCOUNT_TYPE = "account_type";
  public static final String CLIENT_TIMEOUT_MS = "client_timeout_ms";
  public static final String RUNTIME_ERROR_TOPIC_BOOTSTRAP_SERVERS =
      "runtime.error.topic.bootstrap.servers";
  public static final String RUNTIME_ERROR_TOPIC_NAME = "runtime.error.topic.name";
  public static final String FORMAT = "format";
  public static final String MODE = "mode";
  public static final String PARTITION_WINDOW_TYPE = "partition_window_type";
  public static final String TIME_ZONE = "time_zone";

  //  default value
  public static final long DEFAULT_CLIENT_TIME_OUT_MS = 11 * 60 * 60 * 1000; // 11 hour
  public static final String DEFAULT_STS_ENDPOINT = "sts.aliyuncs.com";

  public MaxComputeSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);
  }

  public MaxComputeSinkConnectorConfig(Map<String, String> parsedConfig) {
    this(conf(), parsedConfig);
  }

  public static ConfigDef conf() {
    ConfigDef configDef = new ConfigDef();
    configDef
        .define(MAXCOMPUTE_ENDPOINT,
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
                "Aliyun access key")
        .define(ACCOUNT_ID,
                Type.STRING,
                "",
                Importance.HIGH,
                "Account id for STS")
        .define(REGION_ID,
                Type.STRING,
                "",
                Importance.HIGH,
                "Region id for STS")
        .define(STS_ENDPOINT,
                Type.STRING,
                DEFAULT_STS_ENDPOINT,
                Importance.HIGH,
                "Sts endpoint")
        .define(ROLE_NAME,
                Type.STRING,
                "",
                Importance.HIGH,
                "Role name for STS")
        .define(ACCOUNT_TYPE,
                Type.STRING,
                Account.AccountProvider.ALIYUN.toString(),
                Importance.HIGH,
                "Account type: STS Authorization (STS) or Primary Aliyun Account (ALIYUN)")
        .define(CLIENT_TIMEOUT_MS,
                Type.LONG,
                DEFAULT_CLIENT_TIME_OUT_MS,
                Importance.MEDIUM,
                "STS token time out")
        .define(RUNTIME_ERROR_TOPIC_BOOTSTRAP_SERVERS,
                Type.STRING,
                "",
                Importance.MEDIUM,
                "Bootstrap servers")
        .define(RUNTIME_ERROR_TOPIC_NAME,
                Type.STRING,
                "",
                Importance.MEDIUM,
                "Error topic name")
        .define(FORMAT,
                Type.STRING,
                "TEXT",
                Importance.HIGH,
                "Input format, could be TEXT or CSV")
        .define(MODE,
                Type.STRING,
                "DEFAULT",
                Importance.HIGH,
                "Mode, could be default, key, or value")
        .define(PARTITION_WINDOW_TYPE,
                Type.STRING,
                "HOUR",
                Importance.HIGH,
                "Partition window type, could be DAY, HOUR")
        .define(TIME_ZONE,
                Type.STRING,
                TimeZone.getDefault().getID(),
                Importance.HIGH,
                "Timezone");
    return configDef;
  }
}
