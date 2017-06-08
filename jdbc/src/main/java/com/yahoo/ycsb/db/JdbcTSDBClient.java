package com.yahoo.ycsb.db;

import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.TSDB;

import java.sql.*;
import java.util.List;
import java.util.Properties;

/**
 * TODO.
 */
public class JdbcTSDBClient extends TSDB {

  private Connection conn;

  private String fieldId;
  private String fieldTs;
  private String fieldKind;
  private String fieldPayload;

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
    StringBuilder insertSql = new StringBuilder("INSERT INTO ")
        .append(table).append(" (")
        .append(fieldId).append(",")
        .append(fieldTs).append(",")
        .append(fieldKind).append(",")
        .append(fieldPayload).append(") VALUES (?,?,?,?)");
    try {
      PreparedStatement insertStmt = conn.prepareStatement(insertSql.toString());
      insertStmt.setLong(1, record.getId());
      insertStmt.setLong(2, record.getTimestamp());
      insertStmt.setString(3, record.getKind());
      insertStmt.setString(4, record.getPayload());

      if (insertStmt.executeUpdate() != 1) {
        return Status.UNEXPECTED_STATE;
      }
      return Status.OK;
    } catch (SQLException e) {
      System.err.println("Error in processing insert to table: " + table + e);
      return Status.ERROR;
    }
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
