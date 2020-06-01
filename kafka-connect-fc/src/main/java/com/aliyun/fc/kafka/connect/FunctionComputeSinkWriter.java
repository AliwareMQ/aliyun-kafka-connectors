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

package com.aliyun.fc.kafka.connect;

import com.alibaba.fastjson.JSON;
import com.aliyun.fc.kafka.connect.enums.InvocationTypeEnum;
import com.aliyuncs.fc.client.FunctionComputeClient;
import com.aliyuncs.fc.constants.Const;
import com.aliyuncs.fc.exceptions.ClientException;
import com.aliyuncs.fc.exceptions.ServerException;
import com.aliyuncs.fc.request.InvokeFunctionRequest;
import com.aliyuncs.fc.response.InvokeFunctionResponse;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;


public class FunctionComputeSinkWriter implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(FunctionComputeSinkWriter.class);
  private static final String EVENT_PREFIX = "[";
  private static final String EVENT_SUFFIX = "]";
  private static final String EVENT_SPLIT = ",";

  FunctionComputeClient fcClient;
  private String service;
  private String function;
  private String qualifier;
  private InvocationTypeEnum invocationType;
  private int batchSize;
  private int maxRequestSize;
  private Queue<String> queuedRecords;
  private int safeAppendLen;
  private KafkaWriter runtimeErrorWriter;

  public FunctionComputeSinkWriter(FunctionComputeClient fcClient,
                                   String service,
                                   String function,
                                   String qualifier,
                                   InvocationTypeEnum invocationType,
                                   int batchSize,
                                   int maxRequestSize,
                                   KafkaWriter runtimeErrorWriter) {
    this.fcClient = fcClient;
    this.service = service;
    this.function = function;
    this.qualifier = qualifier;
    this.invocationType = invocationType;
    this.batchSize = batchSize;
    this.maxRequestSize = maxRequestSize;
    this.queuedRecords = new LinkedList<>();
    this.safeAppendLen = maxRequestSize - EVENT_SUFFIX.length();
    this.runtimeErrorWriter = runtimeErrorWriter;
  }

  public void write(Collection<SinkRecord> records) {
    for (SinkRecord record : records) {
      write(record);
    }
    trySend();
  }

  private void write(SinkRecord sinkRecord) {
    Record record = new Record();
    record.setTimestamp(sinkRecord.timestamp());
    record.setTopic(sinkRecord.topic());
    record.setPartition(sinkRecord.kafkaPartition());
    record.setOffset(sinkRecord.kafkaOffset());
    record.setKey(sinkRecord.key());
    String value = sinkRecord.value().toString();
    record.setValueSize(value.length());
    if (value.length() > maxRequestSize) {
      record.setOverflowFlag(true);
    } else {
      record.setValue(sinkRecord.value());
    }

    if (queuedRecords.size() < batchSize) {
      queuedRecords.add(JSON.toJSONString(record));
    } else {
      trySend();
    }
  }

  private void trySend() {
    // send request
    if (queuedRecords.isEmpty()) {
      return;
    }

    final StringBuilder sb = new StringBuilder(EVENT_PREFIX);
    while (!queuedRecords.isEmpty() &&
            (sb.length() + queuedRecords.peek().length() + 1) < safeAppendLen) {
      // add the comma delimiter
      if (sb.length() > EVENT_PREFIX.length()) {
        sb.append(EVENT_SPLIT);
      }
      sb.append(queuedRecords.poll());
    }
    sb.append(EVENT_SUFFIX);
    executeRequest(sb.toString());

    trySend();
  }

  private void executeRequest(String payload) {
    LOG.debug("Starting execute request");
    try {
      InvokeFunctionRequest invkReq = new InvokeFunctionRequest(service, function);
      invkReq.setQualifier(qualifier);
      invkReq.setPayload(payload.getBytes());
      if (invocationType.equals(InvocationTypeEnum.ASYNC)) {
        invkReq.setInvocationType(Const.INVOCATION_TYPE_ASYNC);
      }
      InvokeFunctionResponse invkResp = fcClient.invokeFunction(invkReq);
      if (invkResp.getStatus() != HttpURLConnection.HTTP_OK && invkResp.getStatus() != HttpURLConnection.HTTP_ACCEPTED) {
        // todo error handling
        LOG.error("Execute request error, response detail:{}", invkResp);
      }
      reportRuntimeError(payload, new Exception("NO Exception"));
    } catch (Exception e) {
      reportRuntimeError(payload, e);
    }
  }

  private void reportRuntimeError(String payload, Exception e) {
    if (runtimeErrorWriter != null) {
      LOG.info("Thread(" + Thread.currentThread().getId() + ") report runtime error, error detail:", e);

      List<Record> records = JSON.parseArray(payload, Record.class);
      for (Record record: records) {
        runtimeErrorWriter.write(record);
      }
    } else {
      throw new RuntimeException(e);
    }
  }

    public void close() {
    LOG.info("Close FunctionComputeSinkWriter");
  }
}
