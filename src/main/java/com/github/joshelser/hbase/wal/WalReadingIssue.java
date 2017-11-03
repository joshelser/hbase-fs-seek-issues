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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WAL.Reader;
import org.apache.hadoop.hbase.wal.WALFactory;

public class WalReadingIssue {

  public static void readWal(FileSystem fs, Path p) throws IOException {
    if (!fs.exists(p)) {
      throw new RuntimeException(p + " does not exist on the target filesystem (" + fs.getClass() + ")");
    }

    FSDataInputStream fdis = fs.open(p);
    try {
      Reader r = WALFactory.createReader(fs, p, fs.getConf());
      Entry walEntry = r.next();
      while (walEntry != null) {
        System.out.println(walEntry);
        walEntry = r.next();
      }
    } finally {
      fdis.close();
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Args: <path_to_wal>");
      System.exit(1);
    }

    Configuration conf = new Configuration();
    Path p = new Path(args[0]);
    FileSystem fs = p.getFileSystem(conf);

    readWal(fs, p);
  }
}
