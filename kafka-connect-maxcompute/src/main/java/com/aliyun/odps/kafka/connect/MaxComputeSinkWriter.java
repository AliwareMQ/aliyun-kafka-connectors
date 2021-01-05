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
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.TimeZone;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.kafka.connect.converter.RecordConverter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelBufferedWriter;


public class MaxComputeSinkWriter implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(MaxComputeSinkWriter.class);

  private static final DateTimeFormatter DATETIME_FORMATTER =
      DateTimeFormatter.ofPattern("MM-dd-yyyy HH:mm:ss");

  /*
    Internal states of this sink writer, could change
   */
  private boolean closed = false;
  private Long minOffset;
  private UploadSession session;
  private PartitionSpec partitionSpec;
  private RecordWriter writer;
  private Record reusedRecord;
  private Long partitionStartTimestamp;

  /*
    Configs of this sink writer, won't change
   */
  private Odps odps;
  private TableTunnel tunnel;
  private String project;
  private String table;
  private int bufferSize;
  private RecordConverter converter;
  private PartitionWindowType partitionWindowType;
  private TimeZone tz;

  /*
    Performance metrics
   */
  private long totalBytesByClosedSessions = 0;

  public MaxComputeSinkWriter(
      Odps odps,
      String project,
      String table,
      RecordConverter converter,
      int bufferSize,
      PartitionWindowType partitionWindowType,
      TimeZone tz) {
    this.odps = Objects.requireNonNull(odps).clone();
    this.tunnel = new TableTunnel(odps);
    this.project = Objects.requireNonNull(project);
    this.table = Objects.requireNonNull(table);
    this.converter = Objects.requireNonNull(converter);
    this.bufferSize = bufferSize;
    this.partitionWindowType = partitionWindowType;
    this.tz = Objects.requireNonNull(tz);
  }

  public void write(SinkRecord sinkRecord, Long timestamp) throws IOException {
    if (minOffset == null) {
      minOffset = sinkRecord.kafkaOffset();
    }

    try {
      resetUploadSessionIfNeeded(timestamp);
    } catch (OdpsException e) {
      throw new IOException(e);
    }

    converter.convert(sinkRecord, reusedRecord);
    writer.write(reusedRecord);
  }

  /**
   * Return the minimum uncommitted offset
   */
  public long getMinOffset() {
    return minOffset;
  }

  /**
   * Refresh the STS access key ID & secret.
   *
   * @param odps odps
   */
  public void refresh(Odps odps) {
    LOGGER.info("Enter refresh.");
    this.odps = odps;
    this.tunnel = new TableTunnel(odps);

    // Upload session may not exist
    if (session != null) {
      String sessionId = session.getId();
      try {
        this.session = tunnel.getUploadSession(project, table, partitionSpec, sessionId);
      } catch (Exception e) {
        LOGGER.error("Set session failed!!!", e);
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Close the writer and commit data to MaxCompute
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    LOGGER.debug("Enter Writer.close()!");
    if (closed) {
      return;
    }
    LOGGER.debug("Prepare closeCurrentSessionWithRetry!");

    closeCurrentSessionWithRetry(3);
    closed = true;
  }

  public long getTotalBytes() {
    if (writer != null) {
      try {
        return totalBytesByClosedSessions + ((TunnelBufferedWriter) writer).getTotalBytes();
      } catch (IOException e) {
        // Writer has been closed, ignore
      }
    }

    return totalBytesByClosedSessions;
  }

  private void closeCurrentSessionWithRetry(int retryLimit) throws IOException {
    LOGGER.debug("Enter closeCurrentSessionWithRetry!");
    if (session == null) {
      return;
    }

    totalBytesByClosedSessions += ((TunnelBufferedWriter) writer).getTotalBytes();
    writer.close();
    LOGGER.debug("writer.close() successfully!");

    while (true) {
      try {
        session.commit();
        LOGGER.debug("session.commit() successfully!");
        break;
      } catch (TunnelException e) {
        // TODO: random backoff
        retryLimit -= 1;
        LOGGER.debug(String.format("retryLimit: %d", retryLimit));
        if (retryLimit >= 0) {
          LOGGER.warn("Failed to commit upload session, retrying", e);
        } else {
          throw new IOException(e);
        }
      }
    }
  }

  private void resetUploadSessionIfNeeded(Long timestamp) throws OdpsException, IOException {
    if (needToResetUploadSession(timestamp)) {
      LOGGER.info("Reset upload session, last timestamp: {}, current: {}",
                  partitionStartTimestamp, timestamp);

      closeCurrentSessionWithRetry(3);

      PartitionSpec partitionSpec = getPartitionSpec(timestamp);
      createPartition(odps, project, table, partitionSpec);
      this.partitionSpec = partitionSpec;

      session = tunnel.createUploadSession(project, table, partitionSpec);
      writer = session.openBufferedWriter(true);
      reusedRecord = session.newRecord();
      ((TunnelBufferedWriter) writer).setBufferSize(bufferSize * 1024 * 1024);
      resetPartitionStartTimestamp(timestamp);
    }
  }

  private PartitionSpec getPartitionSpec(Long timestamp) {
    PartitionSpec partitionSpec = new PartitionSpec();
    ZonedDateTime dt = Instant.ofEpochSecond(timestamp).atZone(tz.toZoneId());
    String datetimeString = dt.format(DATETIME_FORMATTER);

    switch (partitionWindowType) {
      case DAY:
        partitionSpec.set("pt", datetimeString.substring(0, 10));
        break;
      case HOUR:
        partitionSpec.set("pt", datetimeString.substring(0, 13));
        break;
      case MINUTE:
        partitionSpec.set("pt", datetimeString.substring(0, 16));
        break;
      default:
        throw new RuntimeException("Unsupported partition window type");
    }

    return partitionSpec;
  }

  private boolean needToResetUploadSession(Long timestamp) {
    if (session == null) {
      return true;
    }

    switch (partitionWindowType) {
      case DAY:
        return timestamp >= partitionStartTimestamp + 24 * 60 * 60;
      case HOUR:
        return timestamp >= partitionStartTimestamp + 60 * 60;
      case MINUTE:
        return timestamp >= partitionStartTimestamp + 60;
      default:
        throw new RuntimeException("Unsupported partition window type");
    }
  }

  private void resetPartitionStartTimestamp(Long timestamp) {
    if (partitionStartTimestamp == null) {
      ZonedDateTime dt = Instant.ofEpochSecond(timestamp).atZone(tz.toZoneId());
      ZonedDateTime partitionStartDatetime;
      switch (partitionWindowType) {
        case DAY:
          partitionStartDatetime =
              ZonedDateTime.of(dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(),
                               0, 0, 0, 0, tz.toZoneId());
          break;
        case HOUR:
          partitionStartDatetime =
              ZonedDateTime.of(dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(),
                               dt.getHour(), 0, 0, 0, tz.toZoneId());
          break;
        case MINUTE:
          partitionStartDatetime =
              ZonedDateTime.of(dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(),
                               dt.getHour(), dt.getMinute(), 0, 0, tz.toZoneId());
          break;
        default:
          throw new RuntimeException("Unsupported partition window type");
      }

      partitionStartTimestamp = partitionStartDatetime.toEpochSecond();
    }
  }

  private static synchronized void createPartition(
      Odps odps,
      String project,
      String table,
      PartitionSpec partitionSpec)
      throws OdpsException {
    Table t = odps.tables().get(project, table);
    // Check the existence of the partition before executing a DML. Could save a lot of time.
    if (!t.hasPartition(partitionSpec)) {
      // Add if not exists to avoid conflicts
      t.createPartition(partitionSpec, true);
    }
  }
}
