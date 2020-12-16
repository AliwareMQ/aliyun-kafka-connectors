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


import com.aliyun.odps.TableSchema;

public class RecordConverterBuilder {
  public enum Mode {
    /**
     * In this mode, both key and value will be saved to a MaxCompute record
     */
    DEFAULT,
    /**
     * In this mode, the message key will be converted to a MaxCompute record
     */
    KEY,
    /**
     * In this mode, the message value will be converted to a MaxCompute record
     */
    VALUE
  }

  public enum Format {
    /**
     * Format is binary
     */
    BINARY,
    /**
     * Format is text
     */
    TEXT,
    /**
     * Format is csv
     */
    CSV,
  }

  private Mode mode = Mode.DEFAULT;
  private Format format = Format.TEXT;
  private TableSchema schema = null;

  public RecordConverterBuilder() {
  }

  public RecordConverterBuilder mode(Mode mode) {
    this.mode = mode;
    return this;
  }

  public RecordConverterBuilder format(Format format) {
    this.format = format;
    return this;
  }

  public RecordConverterBuilder schema(TableSchema schema) {
    this.schema = schema;
    return this;
  }

  public RecordConverter build() {
    if (Format.TEXT.equals(format)) {
      return new DefaultRecordConverter(mode);
    } else if (Format.BINARY.equals(format)) {
      return new BinaryRecordConverter(mode);
    } else if (Format.CSV.equals(format) && !Mode.DEFAULT.equals(mode)) {
      if (schema == null) {
        throw new IllegalArgumentException("Required argument: schema");
      }
      return new CsvRecordConverter(schema, mode);
    } else {
      throw new IllegalArgumentException(
          "Unsupported combination, Converter type: " + mode + ", format: " + format);
    }
  }
}
