package com.voole.hobbit2.kafka.camus.common;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public interface ICamusKey {

	public void setTopic(String topic);

	public int getPartition();

	public void setPartition(int partition);

	public long getStamp();

	public void setStamp(long stamp);

	public long getOffset();

	public void setOffset(long offset);

	public MapWritable getPartitionMap();

	public void put(Writable key, Writable value);

	public long getChecksum();

	public void setChecksum(long checksum);

	public long getBeginOffset();

	public void setBeginOffset(long beginOffset);

}
