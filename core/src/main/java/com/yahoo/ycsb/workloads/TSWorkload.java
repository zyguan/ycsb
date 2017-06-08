package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.*;
import com.yahoo.ycsb.measurements.Measurements;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TODO.
 */
public class TSWorkload extends Workload {

  public static final String TABLENAME_PROPERTY = "table";
  public static final String DEFAULT_TABLENAME_PROPERTY = "t_record";
  protected String table;

  public static final String RECORD_NO_KINDS_PROPERTY = "tsrecord.kind.size";
  public static final String DEFAULT_RECORD_NO_KINDS = "20";
  protected int nKinds;

  public static final String RECORD_PAYLOAD_SIZE_PROPERTY = "tsrecord.payload.size";
  public static final String DEFAULT_RECORD_PAYLOAD_SIZE = "100";

  public static final String RECORD_INIT_ID_PROPERTY = "tsrecord.id.start";
  public static final String DEFAULT_RECORD_INIT_ID = "1";
  protected long initId;

  public static final String RECORD_INIT_TS_PROPERTY = "tsrecord.ts.start";
  public static final String DEFAULT_RECORD_INIT_TS = "1483228800";
  protected long initTs;

  public static final String TS_STEP_PROPERTY = "tsrecord.ts.step_mean";
  public static final String DEFAULT_TS_STEP = "0.2";
  protected double step;

  public static final String TS_STDDEV_PROPERTY = "tsrecord.ts.step_stddev";
  public static final String DEFAULT_TS_STDDEV = "5.0";
  protected double stddev;

  public static final String TS_SEED_PROPERTY = "tsrecord.seed";
  public static final String DEFAULT_TS_SEED = "42";

  public static final String QUERY_WIND_PROPERTY = "query.window";
  public static final String DEFAULT_QUERY_WIND = "300";
  protected long queryWind;

  public static final String QUERY_PROP_PROPERTY = "query.proportion";
  public static final String DEFAULT_QUERY_PROP = "0.5";
  protected double queryProp;

  private int insCntLoad;
  private AtomicInteger insCntRun;
  private TSRecordGenerator insRecordSeq;
  private TSRecordGenerator txnRecordSeq;

  private DiscreteGenerator txnOpChooser;

  private Measurements measurements = Measurements.getMeasurements();

  @Override
  public void init(Properties p) throws WorkloadException {
    table = p.getProperty(TABLENAME_PROPERTY, DEFAULT_TABLENAME_PROPERTY);
    nKinds = Integer.parseInt(p.getProperty(RECORD_NO_KINDS_PROPERTY, DEFAULT_RECORD_NO_KINDS));

    initId = Long.parseLong(p.getProperty(RECORD_INIT_ID_PROPERTY, DEFAULT_RECORD_INIT_ID));
    initTs = Long.parseLong(p.getProperty(RECORD_INIT_TS_PROPERTY, DEFAULT_RECORD_INIT_TS));
    step = Double.parseDouble(p.getProperty(TS_STEP_PROPERTY, DEFAULT_TS_STEP));
    stddev = Double.parseDouble(p.getProperty(TS_STDDEV_PROPERTY, DEFAULT_TS_STDDEV));

    long seed = Long.parseLong(p.getProperty(TS_SEED_PROPERTY, DEFAULT_TS_SEED));
    int payloadSize = Integer.parseInt(p.getProperty(RECORD_PAYLOAD_SIZE_PROPERTY, DEFAULT_RECORD_PAYLOAD_SIZE));

    insCntLoad = Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));
    insRecordSeq = new TSRecordGenerator(initId, initTs, step, stddev, nKinds, seed, payloadSize);

    insCntRun = new AtomicInteger(0);
    txnRecordSeq = new TSRecordGenerator(txnStartId(), txnStartTs(), step, stddev, nKinds, seed+1, payloadSize);

    queryWind = Long.parseLong(p.getProperty(QUERY_WIND_PROPERTY, DEFAULT_QUERY_WIND));
    queryProp = Double.parseDouble(p.getProperty(QUERY_PROP_PROPERTY, DEFAULT_QUERY_PROP));
    txnOpChooser = new DiscreteGenerator();
    txnOpChooser.addValue(queryProp, "QUERY");
    if (1-queryProp > 0) {
      txnOpChooser.addValue(1-queryProp, "INSERT");
    }
  }

  long txnStartId() {
    return initId + insCntLoad;
  }

  long txnStartTs() {
    return initTs+Math.round(step* insCntLoad);
  }

  @Override
  public boolean doInsert(DB db, Object threadstate) {
    if (db instanceof DBWrapper) {
      db = ((DBWrapper) db).getInternalDB();
    }
    if (!(db instanceof TSDB)) {
      throw new UnsupportedOperationException(db + " isn't an instance of TSDB");
    }

    return doTxnInsert((TSDB) db, insRecordSeq);
  }

  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    if (db instanceof DBWrapper) {
      db = ((DBWrapper) db).getInternalDB();
    }
    if (!(db instanceof TSDB)) {
      throw new UnsupportedOperationException(db + " isn't an instance of TSDB");
    }
    TSDB tsdb = (TSDB) db;
    switch (txnOpChooser.nextValue()) {
    case "QUERY":
      doTxnQuery(tsdb);
      break;
    case "INSERT":
      doTxnInsert(tsdb, txnRecordSeq);
      break;
    default:
      return false;
    }
    return true;
  }

  boolean doTxnQuery(TSDB db) {
    long t = initTs + Utils.random().nextInt((int) Math.round(step*(insCntLoad + insCntRun.get())));
    long d = Math.round(.5 * queryWind);

    long startTime = System.nanoTime();

    Status status = db.query(table, t-d, t+d,
        TSRecordGenerator.nthKind(Utils.random().nextInt(nKinds)), null);

    measurements.measure("QUERY", (int) (System.nanoTime()-startTime)/1000);
    measurements.reportStatus("QUERY", status);

    return status != null && status.isOk();
  }

  boolean doTxnInsert(TSDB db, TSRecordGenerator seq) {
    long startTime = System.nanoTime();

    Status status = db.insert(table, seq.nextValue());

    measurements.measure("INSERT", (int) (System.nanoTime()-startTime)/1000);
    measurements.reportStatus("INSERT", status);

    insCntRun.incrementAndGet();
    return status != null && status.isOk();
  }
}
