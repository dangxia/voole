package com.voole.hobbit2.storm.order.state;

import java.util.List;

import org.apache.avro.specific.SpecificRecordBase;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class ExtraInfoQueryStateFunction extends
		BaseQueryFunction<ExtraInfoQueryState, SpecificRecordBase> {
	public static final Fields INPUT_FIELDS = new Fields("data");
	public static final Fields OUTPUT_FIELDS = new Fields("dry");

	public static Stream query(Stream stream, TridentState state) {
		return stream.stateQuery(state, INPUT_FIELDS,
				new ExtraInfoQueryStateFunction(), OUTPUT_FIELDS).project(
				OUTPUT_FIELDS);
	}

	@Override
	public List<SpecificRecordBase> batchRetrieve(ExtraInfoQueryState state,
			List<TridentTuple> tuples) {
		return state.query(tuples);
	}

	@Override
	public void execute(TridentTuple tuple, SpecificRecordBase result,
			TridentCollector collector) {
		collector.emit(new Values(result));

	}

}
