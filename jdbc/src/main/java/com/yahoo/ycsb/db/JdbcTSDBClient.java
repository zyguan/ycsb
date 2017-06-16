package com.yahoo.ycsb.db;

import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.TSDB;
import com.yahoo.ycsb.workloads.TSWorkload;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * TODO.
 */
public class JdbcTSDBClient extends TSDB {

  private Connection conn;

  private String fieldId;
  private String fieldTs;
  private String fieldKind;
  private String fieldPayload;

  public static final String TSDB_BATCHSIZE_PROPERTY = "tsdb.batchsize";
  public static final String DEFAULT_TSDB_BATCHSIZE = "2000";
  protected int batchSize;

  protected ConcurrentLinkedQueue<TSRecord> cachedRecords;

  @Override
  public void init() throws DBException {

    Properties props = getProperties();

    fieldId = props.getProperty(RECORD_ID_FIELD_PROPERTY, DEFAULT_RECORD_ID_FIELD);
    fieldTs = props.getProperty(RECORD_TS_FIELD_PROPERTY, DEFAULT_RECORD_TS_FIELD);
    fieldKind = props.getProperty(RECORD_KIND_FIELD_PROPERTY, DEFAULT_RECORD_KIND_FIELD);
    fieldPayload = props.getProperty(RECORD_PAYLOAD_FIELD_PROPERTY, DEFAULT_RECORD_PAYLOAD_PROPETY);

    String driver = props.getProperty(JdbcDBClient.DRIVER_CLASS);
    String url = props.getProperty(JdbcDBClient.CONNECTION_URL);
    String user = props.getProperty(JdbcDBClient.CONNECTION_USER);
    String passwd = props.getProperty(JdbcDBClient.CONNECTION_PASSWD);

    cachedRecords = new ConcurrentLinkedQueue<>();
    batchSize = Integer.parseInt(props.getProperty(TSDB_BATCHSIZE_PROPERTY, DEFAULT_TSDB_BATCHSIZE));

    try {
      if (driver != null) {
        Class.forName(driver);
      }
      conn = DriverManager.getConnection(url, user, passwd);
      conn.setAutoCommit(true);
    } catch (ClassNotFoundException e) {
      System.err.println("Error in initializing the JDBS driver: " + e);
      throw new DBException(e);
    } catch (SQLException e) {
      System.err.println("Error in database operation: " + e);
      throw new DBException(e);
    }
  }

  @Override
  public void cleanup() throws DBException {
    try {
      String table =   getProperties().getProperty(TSWorkload.TABLENAME_PROPERTY, TSWorkload.DEFAULT_TABLENAME);
      insertRecords(table, new ArrayList<>(cachedRecords));
      conn.close();
    } catch (SQLException e) {
      System.err.println("Error in closing the connection. " + e);
      throw new DBException(e);
    }
  }

  @Override
  public Status query(String table, long start, long end, String kind, List<TSRecord> result) {
    StringBuilder querySql = new StringBuilder("SELECT ")
        .append(fieldId).append(",")
        .append(fieldTs).append(",")
        .append(fieldKind).append(",")
        .append(fieldPayload);
    querySql.append(" FROM ").append(table);
    querySql.append(" WHERE ").append(fieldTs).append(" BETWEEN ? AND ?");
    querySql.append("   AND ").append(fieldKind).append(" = ?");
    querySql.append(" ORDER BY ").append(fieldTs).append(" DESC");

    try {
      PreparedStatement queryStmt = conn.prepareStatement(querySql.toString());
      queryStmt.setLong(1, start);
      queryStmt.setLong(2, end);
      queryStmt.setString(3, kind);

      ResultSet resultSet = queryStmt.executeQuery();
      while (resultSet.next()) {
        if (result != null) {
          result.add(new TSRecord(
              resultSet.getLong(fieldId),
              resultSet.getLong(fieldTs),
              resultSet.getString(fieldKind),
              resultSet.getString(fieldPayload)));
        }
      }
      resultSet.close();
      return Status.OK;
    } catch (SQLException e) {
      System.err.println("Error in processing query to table: " + table + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, TSRecord record) {
    List<TSRecord> records;
    if (batchSize <= 1) {
      records = new ArrayList<>();
      records.add(record);
    } else {
      cachedRecords.offer(record);
      if (cachedRecords.size() < batchSize) {
        return Status.BATCHED_OK;
      }

      records = new ArrayList<>(batchSize);
      while (cachedRecords.size() > 0 && records.size() < batchSize) {
        TSRecord r = cachedRecords.poll();
        if (r == null) {
          break;
        }
        records.add(r);
      }
    }

    try {
      return insertRecords(table, records);
    } catch (SQLException e) {
      System.err.println("Error in processing insert to table(" + table + "): " + e);
      return Status.ERROR;
    }
  }

  private Status insertRecords(String table, List<TSRecord> records) throws SQLException {
    if (records.size() == 0) {
      return Status.OK;
    }
    StringBuilder insertSql = new StringBuilder("INSERT INTO ")
        .append(table).append(" (")
        .append(fieldId).append(",")
        .append(fieldTs).append(",")
        .append(fieldKind).append(",")
        .append(fieldPayload).append(") VALUES (?,?,?,?)");
    for (int i = 1; i < records.size(); i++) {
      insertSql.append(",(?,?,?,?)");
    }
    PreparedStatement insertStmt = conn.prepareStatement(insertSql.toString());
    for (int i = 0; i < records.size(); i++) {
      insertStmt.setLong(1+i*4, records.get(i).getId());
      insertStmt.setLong(2+i*4, records.get(i).getTimestamp());
      insertStmt.setString(3+i*4, records.get(i).getKind());
      insertStmt.setString(4+i*4, records.get(i).getPayload());
    }
    if (insertStmt.executeUpdate() != records.size()) {
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, long id) {
    StringBuilder deleteSql = new StringBuilder("DELETE FROM ").append(table)
        .append(" WHERE ").append(fieldId).append("=?");
    try {
      PreparedStatement deleteStmt = conn.prepareStatement(deleteSql.toString());
      deleteStmt.setLong(1, id);
      if (deleteStmt.executeUpdate() != 1) {
        return Status.UNEXPECTED_STATE;
      }
      return Status.OK;
    } catch (SQLException e) {
      System.err.println("Error in processing delete to table: " + table + e);
      return Status.ERROR;
    }
  }

}
