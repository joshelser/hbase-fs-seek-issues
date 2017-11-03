Create a logj4.properties file and set the necessary DEBUG and TRACE level log changes:
```bash
$ cp /etc/hbase/conf/log4j.properties .
$ vim log4j.properties
```

Run one of the utilities using the `hbase classpath`:
```bash
$ java -cp "$(pwd):hbase-wal-read-0.0.1-SNAPSHOT.jar:$(hbase classpath)" com.github.joshelser.hbase.wal.WalWriteAndSeekIssue
```

Read the console output to see if any issues exist with the seek logic.
