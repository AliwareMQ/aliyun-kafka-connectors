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

import java.util.Objects;

import org.apache.kafka.connect.sink.SinkRecord;

import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Record;


/**
 * Convert a {@link SinkRecord} to a {@link Record} with the following schema:
 *
 * Columns:
 * TOPIC STRING, PARTITION BIGINT, OFFSET BIGINT, [KEY BINARY | VALUE BINARY]
 *
 * Partitioned columns:
 * PT STRING
 *
 */
public class BinaryRecordConverter implements RecordConverter {

  private RecordConverterBuilder.Mode mode;

  public BinaryRecordConverter(RecordConverterBuilder.Mode mode) {
    this.mode = Objects.requireNonNull(mode);
  }

  @Override
  public void convert(SinkRecord in, Record out) {
    out.setString(TOPIC, in.topic());
    out.setBigint(PARTITION, in.kafkaPartition().longValue());
    out.setBigint(OFFSET, in.kafkaOffset());

    switch (mode) {
      case KEY:
        out.set(KEY, convertToBinary(in.key()));
        break;
      case VALUE:
        out.set(VALUE, convertToBinary(in.value()));
        break;
      case DEFAULT:
      default:
        out.set(KEY, convertToBinary(in.key()));
        out.set(VALUE, convertToBinary(in.value()));
    }
  }

  private Binary convertToBinary(Object data) {
      return new Binary(((byte [])data));
  }
}
