/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.tools.kafka.partition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Objects;
import com.voole.hobbit2.tools.kafka.partition.TopicPartition;

/**
 * @author XuehuiHe
 * @date 2014年8月27日
 */
public class CamusKafkaKey implements Serializable, Writable {
	private TopicPartition partition;
	private long offset;
	private long checksum;

	public TopicPartition getPartition() {
		return partition;
	}

	public void setPartition(TopicPartition partition) {
		this.partition = partition;
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

	public void clear() {
		this.partition = null;
		this.offset = 0;
		this.checksum = 0;
	}
	
	public void set(TopicPartition partition,long offset,long checksum){
		setPartition(partition);
		setOffset(offset);
		setChecksum(checksum);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.partition.write(out);
		WritableUtils.writeVLong(out, offset);
		WritableUtils.writeVLong(out, checksum);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		if (this.partition == null) {
			this.partition = new TopicPartition();
		}
		this.partition.readFields(in);
		this.offset = WritableUtils.readVLong(in);
		this.checksum = WritableUtils.readVLong(in);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(partition, offset, checksum);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj != null && obj instanceof CamusKafkaKey) {
			CamusKafkaKey that = (CamusKafkaKey) obj;
			return Objects.equal(this.partition, that.partition)
					&& Objects.equal(this.offset, that.offset)
					&& Objects.equal(this.checksum, that.checksum);
		}
		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("partition", partition)
				.add("offset", offset).add("checksum", checksum).toString();
	}

}
