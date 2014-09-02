package com.voole.hobbit2.camus.meta.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.voole.hobbit2.camus.meta.mapreduce.CamusInputSplit;
import com.voole.hobbit2.kafka.common.IKafkaKey;

public class CamusKey implements WritableComparable<CamusKey>, IKafkaKey {
	private String topic;
	private int partition;
	private long stamp;
	private long offset;
	private MapWritable partitionMap;
	private long checksum;
	private long beginOffset;

	public CamusKey() {
		this.partitionMap = new MapWritable();
	}

	public CamusKey(CamusKey other) {
		this();
		this.topic = other.topic;
		this.partition = other.partition;
		this.partitionMap.putAll(other.partitionMap);
		this.stamp = other.stamp;
		this.offset = other.offset;
		this.checksum = other.checksum;
	}

	public CamusKey(CamusInputSplit split) {
		this();
		this.topic = split.getBrokerAndTopicPartition().getPartition()
				.getTopic();
		this.partition = split.getBrokerAndTopicPartition().getPartition()
				.getPartition();
		this.beginOffset = split.getOffset();
	}

	public void set(long offset, long checksum) {
		this.offset = offset;
		this.checksum = checksum;
	}

	public void clear() {
		this.stamp = 0l;
		this.offset = 0l;
		this.checksum = 0l;
		this.partitionMap.clear();
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
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

	public MapWritable getPartitionMap() {
		return partitionMap;
	}

	public void setPartitionMap(MapWritable partitionMap) {
		this.partitionMap = partitionMap;
	}

	public long getChecksum() {
		return checksum;
	}

	public void setChecksum(long checksum) {
		this.checksum = checksum;
	}

	public long getBeginOffset() {
		return beginOffset;
	}

	public void setBeginOffset(long beginOffset) {
		this.beginOffset = beginOffset;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, topic);
		WritableUtils.writeVInt(out, partition);
		WritableUtils.writeVLong(out, stamp);
		WritableUtils.writeVLong(out, offset);
		WritableUtils.writeVLong(out, checksum);
		this.partitionMap.write(out);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.topic = WritableUtils.readString(in);
		this.partition = WritableUtils.readVInt(in);
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
		return ComparisonChain.start().compare(this.topic, o.topic)
				.compare(this.partition, o.partition)
				.compare(this.beginOffset, o.beginOffset)
				.compare(this.stamp, o.stamp).result();
	}

	@Override
	public void put(Writable key, Writable value) {
		this.partitionMap.put(key, value);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(topic, partition, beginOffset, stamp);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj != null && obj instanceof CamusKey) {
			return compareTo((CamusKey) obj) == 0;
		}
		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("topic", topic)
				.add("partition", partition).add("beginOffset", beginOffset)
				.add("offset", offset).add("stamp", stamp)
				.add("partitionMap", partitionMap).add("checksum", checksum)
				.toString();
	}
}
