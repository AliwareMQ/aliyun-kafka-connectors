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

package com.aliyun.odps.kafka.connect.converter;

import java.util.Date;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import com.aliyun.odps.data.Record;


/**
 * Convert a {@link SinkRecord} to a {@link Record} with the following schema:
 *
 * TOPIC STRING, PARTITION BIGINT, OFFSET BIGINT, KEY STRING, VALUE STRING, DT DATETIME
 */
public class DefaultRecordConverter implements RecordConverter {

  /**
   * Default table column names
   */
  private static final String TOPIC = "topic";
  private static final String PARTITION = "partition";
  private static final String OFFSET = "offset";
  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static final String DT = "dt";

  @Override
  public void convert(SinkRecord in, Long time, Record out) {
    out.setString(TOPIC, in.topic());
    out.setBigint(PARTITION, in.kafkaPartition().longValue());
    out.setBigint(OFFSET, in.kafkaOffset());
    out.setString(KEY, convertKeyToString(in.keySchema(), in.key()));
    out.setString(VALUE, convertValueToString(in.valueSchema(), in.value()));
    out.setDatetime(DT, new Date(time));
  }

  private String convertKeyToString(Schema schema, Object key) {
    if (schema == null) {
      return key == null ? null : key.toString();
    }

    if (schema.type().isPrimitive()) {
      return key == null ? null : key.toString();
    } else {
      throw new RuntimeException("Unsupported type " + schema.type() + ", key: " + key);
    }
  }

  private String convertValueToString(Schema schema, Object value) {
    if (schema == null) {
      return value.toString();
    }

    if (schema.type().isPrimitive()) {
      return value.toString();
    } else {
      throw new RuntimeException("Unsupported type " + schema.type() + ", value: " + value);
    }
  }
}
