package com.voole.hobbit2.camus.mr.partitioner;

public interface ICamusPartitionerManager {

	public ICamusPartitioner findPartitioner(String topic);

	public void register(String topic, ICamusPartitioner partitioner);

	public void encapsulate();
}
