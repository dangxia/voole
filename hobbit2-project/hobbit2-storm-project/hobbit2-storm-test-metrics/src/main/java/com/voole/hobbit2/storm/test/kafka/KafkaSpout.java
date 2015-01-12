package com.voole.hobbit2.storm.test.kafka;

import java.util.Map;

import storm.trident.spout.IOpaquePartitionedTridentSpout;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;

public class KafkaSpout implements
		IOpaquePartitionedTridentSpout<Long, KafkaSpoutPartition, Long> {

	public KafkaSpout() {
	}

	@Override
	public Emitter<Long, KafkaSpoutPartition, Long> getEmitter(Map conf,
			TopologyContext context) {
		return new KafkaEmitter();
	}

	@Override
	public storm.trident.spout.IOpaquePartitionedTridentSpout.Coordinator<Long> getCoordinator(
			Map conf, TopologyContext context) {
		return new KafkaCoordinator();
	}

	@Override
	public Map getComponentConfiguration() {
		return null;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("type", "data");
	}

}
