/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.partition;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;

import com.voole.hobbit2.tools.kafka.partition.BrokerAndTopicPartition;

import storm.trident.spout.ISpoutPartition;

/**
 * @author XuehuiHe
 * @date 2014年9月18日
 */
public class KafkaSpoutPartition implements ISpoutPartition, Serializable {
	private BrokerAndTopicPartition brokerAndTopicPartition;
	private final List<Path> noendPaths;

	public KafkaSpoutPartition() {
		noendPaths = new ArrayList<Path>();
	}

	@Override
	public String getId() {
		return brokerAndTopicPartition.toString();
	}

	public BrokerAndTopicPartition getBrokerAndTopicPartition() {
		return brokerAndTopicPartition;
	}

	public void setBrokerAndTopicPartition(
			BrokerAndTopicPartition brokerAndTopicPartition) {
		this.brokerAndTopicPartition = brokerAndTopicPartition;
	}

	public List<Path> getNoendPaths() {
		return noendPaths;
	}
}
