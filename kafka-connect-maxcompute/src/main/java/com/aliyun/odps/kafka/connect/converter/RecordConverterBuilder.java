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


public class RecordConverterBuilder {
  public enum ConverterType {
    /**
     * In this mode, the target MaxCompute schema is fixed, see
     *
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
     * The key or value will be
     */
    CSV,
    /**
     * The message has schema
     */
    AVRO
  }

  private ConverterType type;

  public RecordConverterBuilder() {
  }

  public RecordConverterBuilder type(ConverterType type) {
    this.type = type;
    return this;
  }

  public RecordConverter build() {
    if (type == null || ConverterType.DEFAULT.equals(type)) {
      return new DefaultRecordConverter();
    } else {
      // TODO: support other converter types
      throw new IllegalArgumentException("Unsupported");
    }
  }
}
