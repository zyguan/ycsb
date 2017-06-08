package com.yahoo.ycsb;

import java.util.*;

/**
 * TODO.
 */
public abstract class TSDB extends DB {

  public static final String RECORD_ID_FIELD_PROPERTY = "tsrecord.id.field";
  public static final String DEFAULT_RECORD_ID_FIELD = "id";
  public static final String RECORD_TS_FIELD_PROPERTY = "tsrecord.timestamp.field";
  public static final String DEFAULT_RECORD_TS_FIELD = "timestamp";
  public static final String RECORD_KIND_FIELD_PROPERTY = "tsrecord.kind.field";
  public static final String DEFAULT_RECORD_KIND_FIELD = "kind";
  public static final String RECORD_PAYLOAD_FIELD_PROPERTY = "tsrecord.payload.field";
  public static final String DEFAULT_RECORD_PAYLOAD_PROPETY = "payload";

  /**
   * TODO.
   */
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

    @Override
    public String toString() {
      return String.format("{id:%d,ts:%d,kind:'%s',payload:'%s'}", id, timestamp, kind, payload);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof TSRecord)) {
        return false;
      }
      TSRecord that = (TSRecord) o;
      return this.id == that.id && this.timestamp == that.timestamp &&
          this.kind.equals(that.kind) && this.payload.equals(that.payload);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(toString());
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    throw new UnsupportedOperationException("TSDB doesn't support generic read");
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
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
