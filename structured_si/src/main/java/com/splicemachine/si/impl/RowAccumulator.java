package com.splicemachine.si.impl;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;

public interface RowAccumulator {
    boolean isOfInterest(Cell value);
    boolean accumulate(Cell value) throws IOException;
    boolean isFinished();
    byte[] result();
    long getBytesVisited();
    boolean isCountStar();
}
