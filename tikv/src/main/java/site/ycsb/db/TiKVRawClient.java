package site.ycsb.db;

import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.codec.CodecDataInput;
import org.tikv.common.codec.CodecDataOutput;
import org.tikv.raw.RawKVClient;
import shade.com.google.protobuf.ByteString;
import site.ycsb.*;
import site.ycsb.workloads.CoreWorkload;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * TiKV RawKV Client.
 */
public class TiKVRawClient extends DB {

  private static TiSession session;
  private static RawKVClient kv;

  private CodecDataOutput buf;

  private Map<String, Integer> fieldIndices;

  public static final String PD_ENDPOINTS_PROPERTY = "tikv.pd";
  public static final String PD_ENDPOINTS_PROPERTY_DEFAULT = "127.0.0.1:2379";

  @Override
  public void init() throws DBException {
    buf = new CodecDataOutput();
    fieldIndices = new HashMap<>();
    int numFields = Integer.parseInt(getProperties().getProperty(
        CoreWorkload.FIELD_COUNT_PROPERTY, CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));
    for (int i = 0; i < numFields; i++) {
      fieldIndices.put("field"+i, i);
    }
    synchronized(this) {
      if (session == null) {
        TiConfiguration config = TiConfiguration.createRawDefault(
            getProperties().getProperty(PD_ENDPOINTS_PROPERTY, PD_ENDPOINTS_PROPERTY_DEFAULT));
        try {
          session = TiSession.create(config);
          kv = session.createRawClient();
        } catch (Exception e) {
          throw new DBException(e);
        }
      }
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      ByteString row = kv.get(getRawKey(table, key));
      if (row.isEmpty()) {
        return Status.NOT_FOUND;
      }
      decodeRow(row.toByteArray(), fields, result);
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    // TODO: impl me
    return null;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      kv.put(getRawKey(table, key), encodeRow(values));
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      kv.put(getRawKey(table, key), encodeRow(values));
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    try {
      kv.delete(getRawKey(table, key));
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  private ByteString getRawKey(String table, String key) {
    return ByteString.copyFromUtf8(table + ":" + key);
  }

  private ByteString encodeRow(Map <String, ByteIterator> values) {
    buf.reset();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      int k = fieldIndices.get(entry.getKey());
      buf.writeByte(k);
      byte[] v = entry.getValue().toArray();
      buf.writeShort(v.length);
      buf.write(v);
    }
    return buf.toByteString();
  }

  private void decodeRow(byte[] raw, Set<String> fields, Map<String, ByteIterator> result) {
    CodecDataInput in = new CodecDataInput(raw);
    while (in.available() > 0) {
      int k = in.readUnsignedByte();
      int len = in.readUnsignedShort();
      String key = "field"+k;
      if (fields == null || fields.contains(key)) {
        result.put(key, new ByteArrayByteIterator(raw, in.currentPos(), len));
      }
      in.skipBytes(len);
    }
  }
}
