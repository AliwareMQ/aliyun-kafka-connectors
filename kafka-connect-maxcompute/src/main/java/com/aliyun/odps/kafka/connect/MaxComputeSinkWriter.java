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

import java.io.Closeable;
import java.io.IOException;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.kafka.connect.converter.RecordConverter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelBufferedWriter;


public class MaxComputeSinkWriter implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(MaxComputeSinkWriter.class);

  private boolean closed = false;
  private UploadSession session;
  private RecordWriter writer;
  private Record reusedRecord;
  private RecordConverter converter;
  private Long minOffset;

  public MaxComputeSinkWriter(TableTunnel tunnel,
                              String project,
                              String table,
                              RecordConverter converter,
                              int bufferSize) throws TunnelException {
    this(tunnel, project, table, null, converter, bufferSize);
  }

  public MaxComputeSinkWriter(TableTunnel tunnel,
                              String project,
                              String table,
                              PartitionSpec partitionSpec,
                              RecordConverter converter,
                              int bufferSize) throws TunnelException {
    init(tunnel, project, table, partitionSpec, bufferSize);
    this.converter = converter;
  }

  public void write(SinkRecord sinkRecord, Long time) throws IOException {
    if (minOffset == null) {
      minOffset = sinkRecord.kafkaOffset();
    }

    converter.convert(sinkRecord, time, reusedRecord);
    writer.write(reusedRecord);
  }

  /**
   * Return the minimum uncommitted offset
   */
  public long getMinOffset() {
    return minOffset;
  }

  /**
   * Close the writer and commit data to MaxCompute
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    try {
      writer.close();
      session.commit();
    } catch (Exception e) {
      throw new IOException(e);
    }
    closed = true;
  }

  private void init(TableTunnel tunnel,
                    String project,
                    String table,
                    PartitionSpec partitionSpec,
                    int bufferSizeInMegaBytes) throws TunnelException {
    try {
      if (partitionSpec != null) {
        session = tunnel.createUploadSession(project, table, partitionSpec);
      } else {
        session = tunnel.createUploadSession(project, table);
      }
    } catch (TunnelException e) {
      throw new RuntimeException(e);
    }
    writer = session.openBufferedWriter(true);
    reusedRecord = session.newRecord();
    ((TunnelBufferedWriter) writer).setBufferSize(bufferSizeInMegaBytes * 1024 * 1024);
  }
}
