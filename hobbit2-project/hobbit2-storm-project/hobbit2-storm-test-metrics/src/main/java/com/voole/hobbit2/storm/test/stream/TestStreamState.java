package com.voole.hobbit2.storm.test.stream;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.tuple.TridentTuple;
import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;

import com.voole.hobbit2.storm.test.stream.StreamTuples.SpoutType;

public class TestStreamState implements State {
	private static final Logger LOG = LoggerFactory
			.getLogger(TestStreamState.class);
	private long total;
	private final String name;

	public TestStreamState(String name) {
		this.name = name;
	}

	@Override
	public void beginCommit(Long txid) {
		LOG.info("------name:{},txid:{} beginCommit", name, txid);
	}

	@Override
	public void commit(Long txid) {
		LOG.info("------name:{},txid:{} commit", name, txid);
	}

	public long update(long sum) {
		total += sum;
		LOG.info("------name:{},sum:{},total:{} ", name, sum, total);
		return total;
	}

	public static class TestStreamStateUpdater extends
			BaseStateUpdater<TestStreamState> {
		private SpoutType type;

		public TestStreamStateUpdater(SpoutType type) {
			this.type = type;
		}

		@Override
		public void updateState(TestStreamState state,
				List<TridentTuple> tuples, TridentCollector collector) {
			long sum = 0;
			for (TridentTuple tridentTuple : tuples) {
				sum += tridentTuple.getLong(1);
			}

			long result = state.update(sum);

			collector.emit(new Values(type, result));
		}

	}

	public static class TestStreamStateFactory implements StateFactory {
		private final String name;

		public TestStreamStateFactory(String name) {
			this.name = name;
		}

		@Override
		public State makeState(Map conf, IMetricsContext metrics,
				int partitionIndex, int numPartitions) {
			return new TestStreamState(name + "_" + partitionIndex);
		}

	}

}
