package com.yahoo.ycsb;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Vector;

public abstract class TSDB extends DB {

  public static final String RECORD_ID_FIELD = "ts.record.id";
  public static final String RECORD_ID_DEFAULT = "id";
  public static final String RECORD_TS_FIELD = "ts.record.timestamp";
  public static final String RECORD_TS_DEFAULT = "timestamp";
  public static final String RECORD_KIND_FIELD = "ts.record.kind";
  public static final String RECORD_KIND_DEFAULT = "kind";
  public static final String RECORD_PAYLOAD_FIELD = "ts.record.payload";
  public static final String RECORD_PAYLOAD_DEFAULT = "payload";

  public static class TSRecord {
    private long id;
    private long timestamp;
    private String kind;
    private String payload;

    public TSRecord(long id, long timestamp, String kind, String payload) {
      this.id = id;
      this.timestamp = timestamp;
      this.kind = kind;
      this.payload = payload;
    }

    public long getId() {
      return id;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public String getKind() {
      return kind;
    }

    public String getPayload() {
      return payload;
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    throw new UnsupportedOperationException("TSDB doesn't support generic read");
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    throw new UnsupportedOperationException("TSDB doesn't support generic scan");
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    throw new UnsupportedOperationException("TSDB doesn't support generic update");
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    throw new UnsupportedOperationException("TSDB doesn't support generic insert");
  }

  @Override
  public Status delete(String table, String key) {
    throw new UnsupportedOperationException("TSDB doesn't support generic delete");
  }

  public abstract Status query(String table, long start, long end, String kind, List<TSRecord> result);

  public abstract Status insert(String table, TSRecord record);

  public abstract Status delete(String table, long id);

}
