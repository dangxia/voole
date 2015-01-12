package com.voole.hobbit2.storm.test.stream;

import java.util.Map;

import storm.trident.spout.IOpaquePartitionedTridentSpout;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;

import com.voole.hobbit2.storm.test.merge.SpoutTuples;

public class TestStreamSpout implements
		IOpaquePartitionedTridentSpout<Long, TestStreamSpoutPartition, Long> {
	final TestStreamSpecial special;

	public TestStreamSpout(TestStreamSpecial special) {
		this.special = special;
	}

	@Override
	public Emitter<Long, TestStreamSpoutPartition, Long> getEmitter(Map conf,
			TopologyContext context) {
		return new TestStreamEmitter(this.special);
	}

	@Override
	public storm.trident.spout.IOpaquePartitionedTridentSpout.Coordinator<Long> getCoordinator(
			Map conf, TopologyContext context) {
		return new TestStreamCoordinator(this.special);
	}

	@Override
	public Map getComponentConfiguration() {
		return null;
	}

	@Override
	public Fields getOutputFields() {
		return special.getOutFields();
	}

}
