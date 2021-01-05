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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;

import org.apache.kafka.connect.sink.SinkRecord;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.csvreader.CsvReader;


/**
 * Convert a {@link SinkRecord} to a {@link Record} with the following schema:
 *
 * Columns:
 * TOPIC STRING, PARTITION BIGINT, OFFSET BIGINT, [Columns described in schema]
 *
 * Partitioned columns:
 * PT STRING
 *
 */
public class CsvRecordConverter implements RecordConverter {

  private static String NULL_TOKEN = "\\N";
  private static DateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private TableSchema schema;
  private RecordConverterBuilder.Mode mode;
  private List<Integer> userColIndex = new LinkedList<>();

  public CsvRecordConverter(TableSchema schema, RecordConverterBuilder.Mode mode) {
    this.schema = schema;
    this.mode = mode;

    // Init userColIndex
    for (int i = 0; i < schema.getColumns().size(); i++) {
      Column c = schema.getColumn(i);
      if (!TOPIC.equalsIgnoreCase(c.getName())
          && !PARTITION.equalsIgnoreCase(c.getName())
          && !OFFSET.equalsIgnoreCase(c.getName())) {
        userColIndex.add(i);
      }
    }
  }

  @Override
  public void convert(SinkRecord in, Record out) throws IOException {
    out.setString(TOPIC, in.topic());
    out.setBigint(PARTITION, in.kafkaPartition().longValue());
    out.setBigint(OFFSET, in.kafkaOffset());

    String data;
    if (RecordConverterBuilder.Mode.KEY.equals(mode)) {
      data = (String) in.key();
    } else if (RecordConverterBuilder.Mode.VALUE.equals(mode)) {
      data = (String) in.value();
    } else {
      throw new RuntimeException("Unsupported mode for CsvConverter: " + mode);
    }


    String[] row = load(data);
    if (out.getColumnCount() - 3 != row.length) {
      throw new RuntimeException("Column count doesn't match: " + data);
    }

    for(int i = 0; i < row.length; ++i) {
      try {
        // Can be cast to an array record. See TableTunnel.UploadSession.newRecord().
        setStringByType((ArrayRecord) out, userColIndex.get(i), row[i]);
      } catch (Exception e) {
        throw new IOException("Parse Error while trans value", e);
      }
    }
  }

  private static String[] load(String data) throws IOException {
    InputStream is = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
    CsvReader csvReader = new CsvReader(new InputStreamReader(is, StandardCharsets.UTF_8), ',');

    String[] row;
    if (csvReader.readRecord()) {
      row = csvReader.getValues();

      for(int i = 0; i < row.length; ++i) {
        if (row[i].equals(NULL_TOKEN)) {
          row[i] = null;
        } else if (trimStringQuotes(row[i]).equals(NULL_TOKEN)) {
          // TODO: bug, could have multi quotes
          row[i] = row[i].substring(1, row[i].length() - 1);
        }
      }

      return row;
    } else {
      throw new RuntimeException("Data cannot be parsed or is empty: " +  data);
    }
  }

  private static String trimStringQuotes(String str) {
    int i = 0;
    for(int len = str.length();
        i < len / 2 && str.charAt(i) == '"' && str.charAt(len - i - 1) == '"'; ++i) {
    }
    return str.substring(i, str.length() - i);
  }

  private static void setStringByType(ArrayRecord ret, int idx, String st) throws ParseException {
    if (st == null) {
      ret.set(idx, null);
    } else {
      switch(ret.getColumns()[idx].getTypeInfo().getOdpsType()) {
        case STRING:
          ret.setString(idx, st);
          break;
        case BIGINT:
          ret.setBigint(idx, Long.valueOf(st));
          break;
        case DOUBLE:
          switch (st) {
            case "nan":
              ret.setDouble(idx, Double.NaN);
              break;
            case "inf":
              ret.setDouble(idx, Double.POSITIVE_INFINITY);
              break;
            case "-inf":
              ret.setDouble(idx, Double.NEGATIVE_INFINITY);
              break;
            default:
              ret.setDouble(idx, Double.valueOf(st));
              break;
          }
          break;
        case BOOLEAN:
          ret.setBoolean(idx, Boolean.valueOf(st));
          break;
        case DATETIME:
          ret.setDatetime(idx, DATETIME_FORMAT.parse(st));
          break;
        case DECIMAL:
          ret.setDecimal(idx, new BigDecimal(st));
          break;
        default:
          throw new RuntimeException("Unsupported type " +
                                     ret.getColumns()[idx].getTypeInfo().getOdpsType());
      }
    }
  }
}
