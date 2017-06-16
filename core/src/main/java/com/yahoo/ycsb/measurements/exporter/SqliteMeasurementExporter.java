package com.yahoo.ycsb.measurements.exporter;

import com.yahoo.ycsb.Client;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.*;
import java.util.Properties;

/**
 * TODO.
 */
public class SqliteMeasurementExporter implements MeasurementsExporter {

  public static final String EXPORT_OUT_PROPERTY = "exporter.sqlite.out";
  public static final String DEFAULT_EXPORT_OUT = "reports.db";

  public static final String TESTNAME_PROPERTY = "testname";

  private Connection conn;

  private long id;
  private long ts;

  public SqliteMeasurementExporter(OutputStream os, Properties props) throws ClassNotFoundException, SQLException {
    ts = System.currentTimeMillis() / 1000;

    Class.forName("org.sqlite.JDBC");
    conn = DriverManager.getConnection("jdbc:sqlite:" + props.getProperty(EXPORT_OUT_PROPERTY, DEFAULT_EXPORT_OUT));

    // provisioning
    Statement stmt = conn.createStatement();
    stmt.executeUpdate("CREATE TABLE IF NOT EXISTS report " +
        "(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, start INTEGER, end INTEGER)");
    stmt.executeUpdate("CREATE TABLE IF NOT EXISTS property " +
        "(rid INTEGER, name TEXT, value TEXT)");
    stmt.executeUpdate("CREATE INDEX IF NOT EXISTS idx_rid_property ON property(rid)");
    stmt.executeUpdate("CREATE TABLE IF NOT EXISTS measurement " +
        "(rid INTEGER, name TEXT, metric TEXT, value NUMERIC)");
    stmt.executeUpdate("CREATE INDEX IF NOT EXISTS idx_rid_measurement ON measurement(rid)");

    // log meta info of this test
    String name = props.getProperty(TESTNAME_PROPERTY, defaultTestName(props));
    PreparedStatement insReport = conn.prepareStatement(
        "INSERT INTO report(name,end) VALUES (?,?)", Statement.RETURN_GENERATED_KEYS);
    insReport.setString(1, name);
    insReport.setLong(2, ts);
    if (insReport.executeUpdate() == 0) {
      throw new SQLException("Failed to create test report, no rows affected.");
    }
    ResultSet rs = insReport.getGeneratedKeys();
    if (rs.next()) {
      id = rs.getLong(1);
    } else {
      throw new SQLException("Can not obtain the id of the test report.");
    }

    PreparedStatement insProps = conn.prepareStatement("INSERT INTO property(rid,name,value) VALUES (?,?,?)");
    for (String prop : props.stringPropertyNames()) {
      insProps.setLong(1, id);
      insProps.setString(2, prop);
      insProps.setString(3, props.getProperty(prop));
      insProps.addBatch();
    }
    insProps.executeBatch();
  }

  @Override
  public void write(String metric, String measurement, int i) throws IOException {
    write(metric, measurement, (double) i);
  }

  @Override
  public void write(String metric, String measurement, double d) throws IOException {
    try {
      PreparedStatement insert = conn.prepareStatement(
          "INSERT INTO measurement(rid,name,metric,value) VALUES (?,?,?,?)");
      insert.setLong(1, id);
      insert.setString(2, measurement);
      insert.setString(3, metric);
      insert.setDouble(4, d);
      insert.executeUpdate();
    } catch (SQLException e) {
      throw new IOException("Failed to insert metric("+metric+"."+measurement+"): " + e.getMessage());
    }

    // update start time
    if (metric.equals("OVERALL") && measurement.equals("RunTime(ms)")) {
      try {
        PreparedStatement update = conn.prepareStatement("UPDATE report SET start=? WHERE id=?");
        update.setLong(1, ts - (long)(d/1000));
        update.setLong(2, id);
        update.executeUpdate();
      } catch (SQLException e) {
        throw new IOException("Failed to update the start time of this report: " + e.getMessage());
      }
    }
  }

  @Override
  public void close() throws IOException {
    try {
      conn.close();
    } catch (SQLException e) {
      throw new IOException(e.getMessage());
    }
  }

  private String defaultTestName(Properties props) {
    return props.getProperty("db") +
        ":" + props.getProperty(Client.MAX_EXECUTION_TIME, "0") +
        ":" + props.getProperty(Client.TARGET_PROPERTY, "0");
  }
}
