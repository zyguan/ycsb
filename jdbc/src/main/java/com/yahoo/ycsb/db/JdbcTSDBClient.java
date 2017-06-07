package com.yahoo.ycsb.db;

import com.sun.org.apache.regexp.internal.RE;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.TSDB;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.sql.*;
import java.util.List;
import java.util.Properties;

public class JdbcTSDBClient extends TSDB {

  private Connection conn;
  private boolean autoCommit;

  private String fieldId;
  private String fieldTs;
  private String fieldKind;
  private String fieldPayload;

  private String querySql;

  @Override
  public void init() throws DBException {

    Properties props = getProperties();

    autoCommit = getBoolProperty(props, JdbcDBClient.JDBC_AUTO_COMMIT, true);

    fieldId = props.getProperty(RECORD_ID_FIELD, RECORD_ID_DEFAULT);
    fieldTs = props.getProperty(RECORD_ID_FIELD, RECORD_TS_DEFAULT);
    fieldKind = props.getProperty(RECORD_KIND_FIELD, RECORD_KIND_DEFAULT);
    fieldPayload = props.getProperty(RECORD_PAYLOAD_FIELD, RECORD_PAYLOAD_DEFAULT);

    String driver = props.getProperty(JdbcDBClient.DRIVER_CLASS);
    String url = props.getProperty(JdbcDBClient.CONNECTION_URL);
    String user = props.getProperty(JdbcDBClient.CONNECTION_USER);
    String passwd = props.getProperty(JdbcDBClient.CONNECTION_PASSWD);


    try {
      if (driver != null) {
        Class.forName(driver);
      }
      conn = DriverManager.getConnection(url, user, passwd);
    } catch (ClassNotFoundException e) {
      System.err.println("Error in initializing the JDBS driver: " + e);
      throw new DBException(e);
    } catch (SQLException e) {
      System.err.println("Error in database operation: " + e);
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
    throw new NotImplementedException(); // TODO
  }

  @Override
  public Status delete(String table, long id) {
    throw new NotImplementedException(); // TODO
  }

  private static boolean getBoolProperty(Properties props, String key, boolean defaultVal) {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      return Boolean.parseBoolean(valueStr);
    }
    return defaultVal;
  }
}
