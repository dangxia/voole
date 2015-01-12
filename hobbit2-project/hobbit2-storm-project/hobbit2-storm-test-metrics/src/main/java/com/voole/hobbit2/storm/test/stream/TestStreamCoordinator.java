package com.voole.hobbit2.storm.test.stream;

class TestStreamCoordinator implements
		storm.trident.spout.IOpaquePartitionedTridentSpout.Coordinator<Long> {

	private TestStreamSpecial special;

	TestStreamCoordinator(TestStreamSpecial special) {
		this.special = special;
	}

	@Override
	public boolean isReady(long txid) {
		return true;
	}

	@Override
	public Long getPartitionsForBatch() {
		return this.special.getPartitions()	;
	}

	@Override
	public void close() {

	}

}