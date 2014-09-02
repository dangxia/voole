package com.voole.hobbit2.camus.meta.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import com.voole.hobbit2.camus.meta.mapreduce.CamusInputSplit;

/**
 * The key for the mapreduce job to pull kafka.
 */
public class CamusKey implements WritableComparable<CamusKey> {

	private CamusInputPartition inputPartition;
	private long stamp;
	private long offset;
	private MapWritable partitionMap;
	private long checksum;

	public CamusKey() {
		partitionMap = new MapWritable();
	}

	public CamusKey(CamusInputSplit inputSplit) {
		this();
		this.inputPartition = new CamusInputPartition(inputSplit);
	}

	public CamusKey(CamusKey other) {
		this();
		this.inputPartition = new CamusInputPartition(other.inputPartition);
		this.partitionMap.putAll(other.partitionMap);
		this.stamp = other.stamp;
		this.offset = other.offset;
		this.checksum = other.checksum;
	}

	public void clear() {
		this.stamp = 0l;
		this.offset = 0l;
		this.checksum = 0l;
		this.partitionMap.clear();
	}

	public void set(long offset, long checksum) {
		this.offset = offset;
		this.checksum = checksum;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.inputPartition.write(out);
		WritableUtils.writeVLong(out, stamp);
		WritableUtils.writeVLong(out, offset);
		WritableUtils.writeVLong(out, checksum);
		this.partitionMap.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		if (this.inputPartition == null) {
			this.inputPartition = new CamusInputPartition();
		}
		this.inputPartition.readFields(in);
		this.stamp = WritableUtils.readVLong(in);
		this.offset = WritableUtils.readVLong(in);
		this.checksum = WritableUtils.readVLong(in);
		if (this.partitionMap == null) {
			this.partitionMap = new MapWritable();
		}
		this.partitionMap.readFields(in);
	}

	@Override
	public int compareTo(CamusKey o) {
		return this.inputPartition.compareTo(o.inputPartition);
	}

	public CamusInputPartition getInputPartition() {
		return inputPartition;
	}

	public void setInputPartition(CamusInputPartition inputPartition) {
		this.inputPartition = inputPartition;
	}

	public long getStamp() {
		return stamp;
	}

	public void setStamp(long stamp) {
		this.stamp = stamp;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public long getChecksum() {
		return checksum;
	}

	public void setChecksum(long checksum) {
		this.checksum = checksum;
	}

	public MapWritable getPartitionMap() {
		return partitionMap;
	}

	public String getTopic() {
		return this.inputPartition.getTopicPartition().getTopic();
	}

	public int getPartition() {
		return this.inputPartition.getTopicPartition().getPartition();
	}

}
