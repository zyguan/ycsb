package com.yahoo.ycsb.generator;

import com.yahoo.ycsb.TSDB;

import java.util.Random;

/**
 * TODO.
 */
public class TSRecordGenerator extends Generator<TSDB.TSRecord> {

  private TSDB.TSRecord lastRecord;
  private TSDB.TSRecord currentRecord;

  private long id;
  private long ts;
  private Random tRNG;
  private Random kRNG;
  private double step;
  private double stddev;
  private int nKinds;
  private Generator<String> payloadGen;

  public TSRecordGenerator(long id, long ts, double step, double stddev, int nKinds, long seed, int payloadSize) {
    this.id = id;
    this.ts = ts;
    this.step = step;
    this.stddev = stddev;
    this.nKinds = nKinds;
    this.tRNG = new Random(seed);
    this.kRNG = new Random(seed);
    this.payloadGen = new IncrementingPrintableStringGenerator(payloadSize);
  }

  private String nextKind() {
    return nthKind(kRNG.nextInt(nKinds));
  }

  public static String nthKind(int n) {
    return String.format("%020d", n);
  }

  @Override
  public synchronized TSDB.TSRecord nextValue() {
    lastRecord = currentRecord;
    currentRecord = new TSDB.TSRecord(id, ts, nextKind(), payloadGen.nextValue());
    id += 1;
    ts += Math.round(step + tRNG.nextGaussian()*stddev);
    return currentRecord;
  }

  @Override
  public synchronized TSDB.TSRecord lastValue() {
    return lastRecord;
  }

  public synchronized TSDB.TSRecord currentValue() {
    return currentRecord;
  }

}