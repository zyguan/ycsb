package com.yahoo.ycsb.generator;

import com.yahoo.ycsb.TSDB;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


public class TSRecordGeneratorTest {

  @Test
  public void initState() {
    TSRecordGenerator gen1 = new TSRecordGenerator(1, 2, 3, 4, 5, 6, 7);
    assertNull(gen1.currentValue());
    assertNull(gen1.lastValue(), null);
    gen1.nextValue();
    assertNotNull(gen1.currentValue());
    assertNull(gen1.lastValue());
  }

  @Test
  public void consumeConcurrently() throws InterruptedException {
    final int n = 100000;
    final int c = 16;
    List<TSDB.TSRecord> exp = new ArrayList<>(n);
    TSRecordGenerator gen1 = new TSRecordGenerator(1, 2, 3, 4, 5, 6, 7);
    while (exp.size() < n) { exp.add(gen1.nextValue()); }

    final List<TSDB.TSRecord> act = new ArrayList<TSDB.TSRecord>(n);
    final TSRecordGenerator gen2 = new TSRecordGenerator(1, 2, 3, 4, 5, 6, 7);
    Thread[] threads = new Thread[c];
    for (int i = 0; i < c; i++) {
      threads[i] = new Thread() {
        @Override
        public void run() {
            while (true) {
              synchronized (act) {
                if (act.size() >= n) {
                  break;
                }
                act.add(gen2.nextValue());
              }
            }
        }
      };
      threads[i].setName("consumer-" + i);
      threads[i].start();
    }
    for (Thread t : threads) { t.join(); }

    Collections.sort(act, new Comparator<TSDB.TSRecord>() {
      @Override
      public int compare(TSDB.TSRecord r1, TSDB.TSRecord r2) {
        return Long.compare(r1.getId(), r2.getId());
      }
    });

    assertEquals(act, exp);
  }
}
