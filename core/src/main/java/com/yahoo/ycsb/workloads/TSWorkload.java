package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.*;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TODO.
 */
public class TSWorkload extends Workload {

  public static final String TABLENAME_PROPERTY = "table";
  public static final String DEFAULT_TABLENAME_PROPERTY = "t_record";
  protected String table;

  public static final String TSRECORD_NO_KINDS_PROPERTY = "tsrecord.kind.size";
  public static final String DEFAULT_TSRECORD_NO_KINDS = "20";
  protected int nKinds;

  protected long initId;
  protected int initInsCnt;

  protected long initTs;
  protected double step;

  protected long queryStdDev;

  private TSRecordGenerator insRecordSeq;
  private TSRecordGenerator txnRecordSeq;
  private AtomicInteger txnInsCnt;

  private DiscreteGenerator txnOpChooser;

  @Override
  public void init(Properties p) throws WorkloadException {
    table = p.getProperty(TABLENAME_PROPERTY, DEFAULT_TABLENAME_PROPERTY);
    nKinds = Integer.parseInt(p.getProperty(TSRECORD_NO_KINDS_PROPERTY, DEFAULT_TSRECORD_NO_KINDS));

    initId = 1;
    initInsCnt = Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));

    initTs = 1483228800;
    step = 0.1;
    double stddev = 5;

    nKinds = 10;
    long seed = 23;

    queryStdDev = 60;

    insRecordSeq = new TSRecordGenerator(initId, initTs, step, stddev, nKinds, seed);
    txnRecordSeq = new TSRecordGenerator(txnStartId(), txnStartTs(), step, stddev, nKinds, seed+1);
    txnInsCnt = new AtomicInteger(0);

    txnOpChooser = new DiscreteGenerator();
    txnOpChooser.addValue(.5, "QUERY");
    txnOpChooser.addValue(.5, "INSERT");
  }

  long txnStartId() {
    return initId + initInsCnt;
  }

  long txnStartTs() {
    return initTs+Math.round(step* initInsCnt);
  }

  @Override
  public boolean doInsert(DB db, Object threadstate) {
    if (db instanceof DBWrapper) {
      db = ((DBWrapper) db).getInternalDB();
    }
    if (!(db instanceof TSDB)) {
      throw new UnsupportedOperationException(db + " isn't an instance of TSDB");
    }

    Status status = ((TSDB) db).insert(table, insRecordSeq.nextValue());

    return status != null && status.isOk();
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
      return doTxnQuery(tsdb);
    case "INSERT":
      return doTxnInsert(tsdb);
    default:
      return false;
    }
  }

  boolean doTxnQuery(TSDB db) {
    long t = initTs + Utils.random().nextInt((int) Math.round(step*(initInsCnt+txnInsCnt.get())));
    long d = Math.round(.5 * queryStdDev * Math.abs(Utils.random().nextGaussian()));
    Status status = db.query(table, t-d, t+d,
        TSRecordGenerator.nthKind(Utils.random().nextInt(nKinds)), null);
    return status != null && status.isOk();
  }

  boolean doTxnInsert(TSDB db) {
    Status status = db.insert(table, txnRecordSeq.nextValue());
    txnInsCnt.incrementAndGet();
    return status != null && status.isOk();
  }
}
