package com.voole.hobbit2.storm.order.state;

import java.io.IOException;
import java.util.List;

import org.apache.avro.specific.SpecificRecordBase;

import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

public interface ExtraInfoQueryState extends State {
	public List<SpecificRecordBase> query(List<TridentTuple> tuples);

	public List<SpecificRecordBase> queryRecords(List<SpecificRecordBase> inputs);

	public void close() throws IOException, InterruptedException;
}
