package com.voole.hobbit2.storm.test.kafka;

class KafkaCoordinator implements
		storm.trident.spout.IOpaquePartitionedTridentSpout.Coordinator<Long> {

	KafkaCoordinator() {
	}

	@Override
	public boolean isReady(long txid) {
		return true;
	}

	@Override
	public Long getPartitionsForBatch() {
		return 3l;
	}

	@Override
	public void close() {

	}

}