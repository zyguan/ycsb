package com.yahoo.ycsb.db;

import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.TSDB;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class InfluxDBClient extends TSDB {

  private InfluxDB db;

  public static final String INFLUXDB_URL_PROPERTY = "influxdb.url";
  public static final String DEFAULT_INFLUXDB_URL = "http://127.0.0.1:8086";

  public static final String INFLUXDB_NAME_PROPERTY = "influxdb.name";
  public static final String DEFAULT_INFLUXDB_NAME = "tsbench";
  protected String dbName;

  public static final String INFLUXDB_BATCH_POINTS_PROPERTY = "influxdb.batch.points";
  public static final String DEFAULT_INFLUXDB_BATCH_POINTS = "2000";

  public static final String INFLUXDB_BATCH_TIMEOUT_PROPERTY = "influxdb.batch.timeout";
  public static final String DEFAULT_INFLUXDB_BATCH_TIMEOUT = "200";

  private String fieldId;
  private String fieldTs;
  private String fieldKind;
  private String fieldPayload;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();

    dbName = props.getProperty(INFLUXDB_NAME_PROPERTY, DEFAULT_INFLUXDB_NAME);

    String url = props.getProperty(INFLUXDB_URL_PROPERTY, DEFAULT_INFLUXDB_URL);
    db = InfluxDBFactory.connect(props.getProperty(INFLUXDB_URL_PROPERTY, DEFAULT_INFLUXDB_URL));
    if (!db.databaseExists(dbName)) {
      throw new DBException(String.format("Database '%s' does not exist on %s", dbName, url));
    }
    int points = Integer.parseInt(props.getProperty(INFLUXDB_BATCH_POINTS_PROPERTY, DEFAULT_INFLUXDB_BATCH_POINTS));
    int timeout = Integer.parseInt(props.getProperty(INFLUXDB_BATCH_TIMEOUT_PROPERTY, DEFAULT_INFLUXDB_BATCH_TIMEOUT));
    db.enableBatch(points, timeout, TimeUnit.MILLISECONDS);

    fieldId = props.getProperty(RECORD_ID_FIELD_PROPERTY, DEFAULT_RECORD_ID_FIELD);
    fieldTs = props.getProperty(RECORD_TS_FIELD_PROPERTY, DEFAULT_RECORD_TS_FIELD);
    fieldKind = props.getProperty(RECORD_KIND_FIELD_PROPERTY, DEFAULT_RECORD_KIND_FIELD);
    fieldPayload = props.getProperty(RECORD_PAYLOAD_FIELD_PROPERTY, DEFAULT_RECORD_PAYLOAD_PROPETY);
  }

  @Override
  public void cleanup() throws DBException {
    db.close();
  }

  @Override
  public Status query(String table, long start, long end, String kind, List<TSRecord> result) {
    throw new NotImplementedException();
  }

  @Override
  public Status insert(String table, TSRecord record) {
    Point p = Point.measurement(table)
        .time(record.getTimestamp(), TimeUnit.SECONDS)
        .addField(fieldId, record.getId())
        .addField(fieldKind, record.getKind())
        .addField(fieldPayload, record.getPayload())
        .build();
    db.write(dbName, "autogen", p);
    return Status.OK;
  }

  @Override
  public Status delete(String table, long id) {
    throw new NotImplementedException();
  }
}
