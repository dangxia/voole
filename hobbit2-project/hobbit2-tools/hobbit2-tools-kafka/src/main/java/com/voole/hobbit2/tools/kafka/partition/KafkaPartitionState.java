/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.tools.kafka.partition;

import com.google.common.base.Objects;

/**
 * @author XuehuiHe
 * @date 2014年8月25日
 */
public class KafkaPartitionState {
	private final KafkaPartition partition;
	private long latestOffset;
	private long earliestOffset;
	private long offset;

	public KafkaPartitionState(KafkaPartition partition) {
		this.partition = partition;
	}

	public KafkaPartition getPartition() {
		return partition;
	}

	public long getLatestOffset() {
		return latestOffset;
	}

	public void setLatestOffset(long latestOffset) {
		this.latestOffset = latestOffset;
	}

	public long getEarliestOffset() {
		return earliestOffset;
	}

	public void setEarliestOffset(long earliestOffset) {
		this.earliestOffset = earliestOffset;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("partition", partition)
				.add("latestOffset", latestOffset)
				.add("earliestOffset", earliestOffset).add("offset", offset)
				.toString();
	}

	public long estimateDataSize() {
		return latestOffset - offset;
	}

}
