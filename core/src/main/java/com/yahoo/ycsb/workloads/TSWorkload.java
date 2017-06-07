package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.TSDB;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.UnixEpochTimestampGenerator;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Properties;

public class TSWorkload extends Workload {

  private UnixEpochTimestampGenerator tsGen;

  @Override
  public void init(Properties p) throws WorkloadException {
  }

  @Override
  public boolean doInsert(DB db, Object threadstate) {
    if (!(db instanceof TSDB)) {
      throw new UnsupportedOperationException("db isn't an instance of TSDB");
    }
    throw new NotImplementedException(); // TODO
  }

  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    if (!(db instanceof TSDB)) {
      throw new UnsupportedOperationException("db isn't an instance of TSDB");
    }
    throw new NotImplementedException(); // TODO
  }
}
