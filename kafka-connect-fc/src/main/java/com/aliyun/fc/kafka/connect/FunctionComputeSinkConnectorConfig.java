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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class FunctionComputeSinkConnectorConfig extends AbstractConfig {

    public static final String REGION = "region";
    public static final String ENDPOINT = "endpoint";
    public static final String SERVICE = "service.name";
    public static final String FUNCTION = "function.name";

    public static final String QUALIFIER = "qualifier";
    public static final String DEFAULT_QUALIFIER = "LATEST";

    public static final String INVOCATION_TYPE = "invocation.type";

    public static final String BATCH_SIZE = "batch.size";
    public static final int DEFAULT_BATCH_SIZE = 20;

    public static final String SYNC_MAX_REQUEST_SIZE = "sync.max.request.size";
    public static final int DEFAULT_SYNC_MAX_REQUEST_SIZE = 6 * 1024 * 1024;    //6M
    public static final String ASYNC_MAX_REQUEST_SIZE = "async.max.request.size";
    public static final int DEFAULT_ASYNC_MAX_REQUEST_SIZE = 128 * 1024;    //128K

    public static final String ACCOUNT_ID = "account.id";
    public static final String STS_ACCESS_ID = "ACCESS_ID";
    public static final String STS_ACCESS_KEY = "ACCESS_KEY";
    public static final String ROLE_NAME = "role.name";

    public static final String CLIENT_TIME_OUT_MS = "client.time.out.ms";
    public static final long DEFAULT_CLIENT_TIME_OUT_MS = 11 * 60 * 60 * 1000; // 11 hour

    public static final String RUNTIME_ERROR_TOPIC_BOOTSTRAP_SERVERS = "runtime.error.topic.bootstrap.servers";
    public static final String RUNTIME_ERROR_TOPIC_NAME = "runtime.error.topic.name";

    public FunctionComputeSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig, false);
    }

    public FunctionComputeSinkConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        ConfigDef configDef = new ConfigDef();
        configDef.define(REGION,
                ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH,
                "FunctionCompute region")
                .define(ENDPOINT,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "FunctionCompute endpoint")
                .define(SERVICE,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "FunctionCompute service")
                .define(FUNCTION,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "FunctionCompute function")
                .define(QUALIFIER,
                        ConfigDef.Type.STRING,
                        DEFAULT_QUALIFIER,
                        ConfigDef.Importance.HIGH,
                        "FunctionCompute qualifier")
                .define(ACCOUNT_ID,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "FunctionCompute account ID")
                .define(INVOCATION_TYPE,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "FunctionCompute invocation type")
                .define(BATCH_SIZE,
                        ConfigDef.Type.INT,
                        DEFAULT_BATCH_SIZE,
                        ConfigDef.Importance.HIGH,
                        "FunctionCompute batch size")
                .define(SYNC_MAX_REQUEST_SIZE,
                        ConfigDef.Type.INT,
                        DEFAULT_SYNC_MAX_REQUEST_SIZE,
                        ConfigDef.Importance.HIGH,
                        "FunctionCompute sync max request size")
                .define(ASYNC_MAX_REQUEST_SIZE,
                        ConfigDef.Type.INT,
                        DEFAULT_ASYNC_MAX_REQUEST_SIZE,
                        ConfigDef.Importance.HIGH,
                        "FunctionCompute async max request size")
                .define(ROLE_NAME,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "Aliyun sts role")
                .define(CLIENT_TIME_OUT_MS,
                        ConfigDef.Type.LONG,
                        DEFAULT_CLIENT_TIME_OUT_MS,
                        ConfigDef.Importance.HIGH,
                        "FunctionCompute client time out")
                .define(RUNTIME_ERROR_TOPIC_BOOTSTRAP_SERVERS,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "Aliyun kafka bootstrap")
                .define(RUNTIME_ERROR_TOPIC_NAME,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "Aliyun runtime error topic name");
        return configDef;
    }
}