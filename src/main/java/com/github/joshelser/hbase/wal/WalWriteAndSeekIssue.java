/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.joshelser.hbase.wal;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.RegionEventDescriptor;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogWriter;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WAL.Reader;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;

import com.google.protobuf.ByteString;

public class WalWriteAndSeekIssue {

  public static Path writeFile(Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);

    // Come up with a unique file
    Path p = createRandomFilePath();
    while (fs.exists(p)) {
      p = createRandomFilePath();
    }

//    final ServerName serverNameProto = ServerName.newBuilder()
//        .setHostName("h1")
//        .setPort(16020)
//        .setStartCode(1)
//        .build();

    ProtobufLogWriter writer = (ProtobufLogWriter) WALFactory.createWALWriter(fs, p, conf);
    TableName tableName = TableName.valueOf("t1");
    HRegionInfo hri = new HRegionInfo(tableName);
    WALKey key = new WALKey(Bytes.toBytes("r1"), tableName);
    RegionEventDescriptor regionEventProto = RegionEventDescriptor.newBuilder()
//        .setRegionName(ByteString.copyFrom(hri.getRegionName()))
//        .setServer(serverNameProto)
        .setTableName(ByteString.copyFrom(tableName.getName()))
        .setEventType(WALProtos.RegionEventDescriptor.EventType.REGION_OPEN)
        .setEncodedRegionName(ByteString.copyFrom(hri.getRegionName()))
        .build();
    WALEdit edit = WALEdit.createRegionEventWALEdit(hri, regionEventProto);
    // Get close to 512B, but don't exceed
    writer.append(new WAL.Entry(key, edit));
    writer.append(new WAL.Entry(key, edit));
    writer.append(new WAL.Entry(key, edit));
    writer.sync();
    // Intentionally bypass close() on the Writer to avoid the WAL trailer from being written.
    writer.getStream().close();

    return p;
  }

  private static Path createRandomFilePath() {
    String username = System.getProperty("user.name");
    if (username == null) {
      throw new RuntimeException("Could not get value of user.name system property");
    }
    // TODO Make sure a PageBlog path is chosen.
    return new Path("/mapreducestaging/" + username, UUID.randomUUID().toString() + ".wal");
  }

  public static void seekAndReadWal(FileSystem fs, Path p) throws IOException {
    if (!fs.exists(p)) {
      throw new RuntimeException(p + " does not exist on the target filesystem (" + fs.getClass() + ")");
    }

    Reader r = WALFactory.createReader(fs, p, fs.getConf());
    r.reset();
    long editsBeginPosition = r.getPosition();
    System.out.println("** Edits begin in file at: " + editsBeginPosition);
    Entry walEntry = r.next();
    while (walEntry != null) {
      System.out.println(walEntry);
      System.out.println("Offset: " + r.getPosition());
      walEntry = r.next();
    }

    r.close();

    r = WALFactory.createReader(fs, p, fs.getConf());
    r.reset();
    System.out.println("** Consuming file");
    walEntry = r.next();
    while (walEntry != null) {
      System.out.println(walEntry);
      System.out.println("** Offset: " + r.getPosition());
      walEntry = r.next();
    }
    System.out.println("** Seeking to the middle of a record");
    r.seek(editsBeginPosition + 5);
    System.out.println("** Trying to read (should fail)");
    walEntry = r.next();
    System.out.println(walEntry);
    System.out.println("** Offset: " + r.getPosition());
    System.out.println("** Trying to read again (should still fail)");
    walEntry = r.next();
    System.out.println(walEntry);
    System.out.println("** Offset: " + r.getPosition());
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Path p = writeFile(conf);
    FileSystem fs = p.getFileSystem(conf);

    seekAndReadWal(fs, p);
  }
}
