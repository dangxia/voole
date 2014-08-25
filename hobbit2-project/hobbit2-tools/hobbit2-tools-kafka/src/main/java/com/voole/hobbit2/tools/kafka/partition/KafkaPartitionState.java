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

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("partition", partition)
				.add("latestOffset", latestOffset)
				.add("earliestOffset", earliestOffset).toString();

	}

}
