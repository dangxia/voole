package com.voole.hobbit.camus.etl;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;


public interface IEtlKey {
    long getTime();

    String getTopic();

    //String getNodeId();

    int getPartition();

    long getBeginOffset();

    long getOffset();

    long getChecksum();

    MapWritable getPartitionMap();

    void put(Writable key, Writable value);
}
