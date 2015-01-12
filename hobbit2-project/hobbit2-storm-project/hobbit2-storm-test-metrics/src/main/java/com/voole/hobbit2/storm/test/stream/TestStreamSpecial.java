package com.voole.hobbit2.storm.test.stream;

import java.io.Serializable;
import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public interface TestStreamSpecial extends Serializable {

	Long getPartitions();

	Values getValues();

	Fields getOutFields();

	List<TestStreamSpoutPartition> getOrderedPartitions(Long allPartitionInfo);
}
