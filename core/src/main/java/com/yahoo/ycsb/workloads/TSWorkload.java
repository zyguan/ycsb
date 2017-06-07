package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.*;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class TSWorkload extends Workload {

  public static class TSRecordGenerator extends Generator<TSDB.TSRecord> {

    private TSDB.TSRecord lastRecord;
    private TSDB.TSRecord currentRecord;

    private AtomicLong id;
    private AtomicLong ts;
    private ThreadLocal<Random> tRNG = new ThreadLocal<>();
    private ThreadLocal<Random> kRNG = new ThreadLocal<>();
    private double step;
    private double stddev;
    private int nKinds;
    private long seed;

    public TSRecordGenerator(long start, double step, double stddev, int nKinds, long seed) {
      id = new AtomicLong(0);
      ts = new AtomicLong(start-Math.round(step));
      this.step = step;
      this.stddev = stddev;
      this.nKinds = nKinds;
      this.seed = seed;
    }

    private long nextTS() {
      Random rand = tRNG.get();
      if (rand == null) {
        rand = new Random(seed);
        tRNG.set(rand);
      }
      return ts.addAndGet(Math.round(step + rand.nextGaussian()*stddev));
    }

    private String nextKind() {
      Random rand = kRNG.get();
      if (rand == null) {
        rand = new Random(seed+42);
        kRNG.set(rand);
      }
      return String.format("%020d", rand.nextInt(nKinds));
    }

    @Override
    public TSDB.TSRecord nextValue() {
      lastRecord = currentRecord;
      currentRecord = new TSDB.TSRecord(id.incrementAndGet(), nextTS(), nextKind(), "TODO");
      return currentRecord;
    }

    @Override
    public TSDB.TSRecord lastValue() {
      return lastRecord;
    }

    public TSDB.TSRecord currentValue() {
      return currentRecord;
    }
  }

  public static final String TABLENAME_PROPERTY = "table";
  public static final String DEFAULT_TABLENAME_PROPERTY = "t_record";
  protected String table;

  private TSRecordGenerator insRecordSeq;
  private TSRecordGenerator txnRecordSeq;

  @Override
  public void init(Properties p) throws WorkloadException {
    table = p.getProperty(TABLENAME_PROPERTY, DEFAULT_TABLENAME_PROPERTY);

    insRecordSeq = new TSRecordGenerator(1483228800, 0.1, 5, 10, 23);
    txnRecordSeq = new TSRecordGenerator(1483228800, 0.1, 5, 10, 23);
    int rcnt = Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));
    for (int i = 0; i < rcnt; i++) { txnRecordSeq.nextValue(); }
  }

  @Override
  public boolean doInsert(DB db, Object threadstate) {
    if (!(db instanceof TSDB)) {
      throw new UnsupportedOperationException(db + " isn't an instance of TSDB");
    }

    Status status = ((TSDB) db).insert(table, insRecordSeq.nextValue());

    return status != null && status.isOk();
  }

  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    if (!(db instanceof TSDB)) {
      throw new UnsupportedOperationException(db + " isn't an instance of TSDB");
    }
    TSDB tsdb = (TSDB) db;

    throw new NotImplementedException(); // TODO
  }


  public static void main(String[] args) {
    TSRecordGenerator rgen = new TSRecordGenerator(1483228800, 0.1, 5, 10, 23);
    for (int i = 0; i < 20; i++) {
      System.out.println(rgen.nextValue());
    }
  }
}
