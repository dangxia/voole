package com.voole.hobbit2.storm.test.merge;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IOpaquePartitionedTridentSpout;
import storm.trident.spout.ISpoutPartition;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.ValueUpdater;
import storm.trident.state.snapshot.Snapshottable;
import storm.trident.topology.TransactionAttempt;
import storm.trident.tuple.TridentTuple;
import backtype.storm.task.IMetricsContext;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;

import com.voole.hobbit2.storm.test.merge.DataSpout.DataSpoutPartition;

public class DataSpout implements
		IOpaquePartitionedTridentSpout<Long, DataSpoutPartition, Long> {
	private final Long kk;

	public DataSpout(Long kk) {
		this.kk = kk;
	}

	@Override
	public storm.trident.spout.IOpaquePartitionedTridentSpout.Emitter<Long, DataSpoutPartition, Long> getEmitter(
			Map conf, TopologyContext context) {
		return new DataSpoutEmitter();
	}

	@Override
	public storm.trident.spout.IOpaquePartitionedTridentSpout.Coordinator<Long> getCoordinator(
			Map conf, TopologyContext context) {
		return new DataSpoutCoordinator();
	}

	@Override
	public Map getComponentConfiguration() {
		return null;
	}

	@Override
	public Fields getOutputFields() {
		return SpoutTuples.SPOUT_OUT_FIELDS;
	}

	class DataSpoutCoordinator
			implements
			storm.trident.spout.IOpaquePartitionedTridentSpout.Coordinator<Long> {

		@Override
		public boolean isReady(long txid) {
			return true;
		}

		@Override
		public Long getPartitionsForBatch() {
			return kk;
		}

		@Override
		public void close() {

		}

	}

	class DataSpoutEmitter
			implements
			storm.trident.spout.IOpaquePartitionedTridentSpout.Emitter<Long, DataSpoutPartition, Long> {
		private Random r;

		public DataSpoutEmitter() {
			r = new Random();
		}

		@Override
		public Long emitPartitionBatch(TransactionAttempt tx,
				TridentCollector collector, DataSpoutPartition partition,
				Long lastPartitionMeta) {
			if (lastPartitionMeta == null) {
				lastPartitionMeta = 0l;
			}
			try {
				TimeUnit.SECONDS.sleep(r.nextInt(3));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			collector.emit(SpoutTuples.getDataValues(1l));
			lastPartitionMeta++;
			return lastPartitionMeta;
		}

		@Override
		public void refreshPartitions(
				List<DataSpoutPartition> partitionResponsibilities) {

		}

		@Override
		public List<DataSpoutPartition> getOrderedPartitions(
				Long allPartitionInfo) {
			List<DataSpoutPartition> list = new ArrayList<DataSpout.DataSpoutPartition>();
			for (int i = 0; i < allPartitionInfo; i++) {
				list.add(new DataSpoutPartition((long) i));
			}
			return list;
		}

		@Override
		public void close() {

		}

	}

	public class DataSpoutPartition implements ISpoutPartition {

		private final long longId;

		private final String id;

		public DataSpoutPartition(Long id) {
			this.longId = id;
			this.id = "DataSpoutPartition_" + kk + "_" + id;
		}

		@Override
		public String getId() {
			return id;
		}

		public long getLongId() {
			return longId;
		}

	}

	public static class DataSpoutStateFactory implements StateFactory {

		@Override
		public State makeState(Map conf, IMetricsContext metrics,
				int partitionIndex, int numPartitions) {
			return new DataSpoutState();
		}
	}

	public static class DataSpoutState implements Snapshottable<Long> {

		private Long max = 0l;
		private Field field;

		public DataSpoutState() {

		}

		public long update(long l) {
			System.out.println("----------update-------------" + max);
			System.out.println("----------l-------------" + l);
			this.max += l;

			return this.max;
		}

		@Override
		public void beginCommit(Long txid) {
			System.out.println("-------------txid:----" + txid);
		}

		@Override
		public void commit(Long txid) {
			System.out.println("-------------commit txid:----" + txid);
		}

		@Override
		public Long get() {
			System.out.println("------------get-----------" + max);
			return max;
		}

		@Override
		public Long update(ValueUpdater updater) {
			if (field == null) {
				try {
					field = updater.getClass().getDeclaredField("arg");
					field.setAccessible(true);
				} catch (NoSuchFieldException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (SecurityException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			try {
				System.out.println("----------args-------------"
						+ field.get(updater));
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("----------update-------------" + max);
			this.max = (Long) updater.update(max);
			return this.max;
		}

		@Override
		public void set(Long o) {
			System.out.println("----------set-------------" + max);
			this.max = o;
		}

	}

	public static class DataSpoutStateUpdater extends
			BaseStateUpdater<DataSpoutState> {

		@Override
		public void updateState(DataSpoutState state,
				List<TridentTuple> tuples, TridentCollector collector) {

			long sum = 0;
			for (TridentTuple tridentTuple : tuples) {
				sum += tridentTuple.getLong(1);
			}
			state.update(sum);
		}

	}

	public static class DataSpoutCombinerAggregator implements
			CombinerAggregator<Long> {

		@Override
		public Long init(TridentTuple tuple) {
			return tuple.getLong(0);
		}

		@Override
		public Long combine(Long val1, Long val2) {
			return val1 + val2;
		}

		@Override
		public Long zero() {
			return 0l;
		}

	}

}
