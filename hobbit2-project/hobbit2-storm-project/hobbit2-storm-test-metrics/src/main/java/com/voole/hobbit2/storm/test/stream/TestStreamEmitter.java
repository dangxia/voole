package com.voole.hobbit2.storm.test.stream;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IOpaquePartitionedTridentSpout.Emitter;
import storm.trident.topology.TransactionAttempt;

class TestStreamEmitter implements
		Emitter<Long, TestStreamSpoutPartition, Long> {
	private Random r;
	final TestStreamSpecial special;

	public TestStreamEmitter(TestStreamSpecial special) {
		r = new Random();
		this.special = special;
	}

	@Override
	public Long emitPartitionBatch(TransactionAttempt tx,
			TridentCollector collector, TestStreamSpoutPartition partition,
			Long lastPartitionMeta) {
		if (lastPartitionMeta == null) {
			lastPartitionMeta = 0l;
		}
		try {
			TimeUnit.SECONDS.sleep(r.nextInt(2));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		collector.emit(special.getValues());
		lastPartitionMeta++;
		return lastPartitionMeta;
	}

	@Override
	public void refreshPartitions(
			List<TestStreamSpoutPartition> partitionResponsibilities) {

	}

	@Override
	public List<TestStreamSpoutPartition> getOrderedPartitions(
			Long allPartitionInfo) {
		return special.getOrderedPartitions(allPartitionInfo);
	}

	@Override
	public void close() {

	}

}