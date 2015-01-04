package com.voole.hobbit2.storm.test;

import java.util.Map;

import backtype.storm.metric.api.CombinedMetric;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.ICombiner;
import backtype.storm.metric.api.IReducer;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.metric.api.MultiReducedMetric;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class NumBolt extends BaseRichBolt {

	private CountMetric countMetric;
	private MultiCountMetric multiCountMetric;
	private CombinedMetric maxMetric;
	private ReducedMetric avgMetric;
	private MultiReducedMetric multiAvgMetric;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
			TopologyContext context, OutputCollector collector) {
		countMetric = context.registerMetric("count", new CountMetric(), 60);
		multiCountMetric = context.registerMetric("multi_count",
				new MultiCountMetric(), 60);
		maxMetric = context.registerMetric("max", new MaxMetric(), 60);
		avgMetric = context.registerMetric("avg", new AvgReduce(), 60);
		multiAvgMetric = context.registerMetric("multi_avg",
				new MultiReducedMetric(new AvgReduce()), 60);
	}

	@Override
	public void execute(Tuple input) {
		Integer num = input.getInteger(0);
		countMetric.incr();
		String key = "simple";
		if (num > 100) {
			key = "thousand";

		} else if (num > 10) {
			key = "ten";
		}
		multiCountMetric.scope(key).incr();
		maxMetric.update(num);
		avgMetric.update(num);
		multiAvgMetric.scope(key).update(num);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	public static class MaxMetric implements ICombiner<Integer> {

		@Override
		public Integer identity() {
			return 0;
		}

		@Override
		public Integer combine(Integer a, Integer b) {
			if (a == null) {
				return b;
			}
			if (b == null) {
				return a;
			}
			return Math.max(a, b);
		}

	}

	public static class AvgState {
		public long count;
		public long total;
	}

	public static class AvgReduce implements IReducer<AvgState> {

		@Override
		public AvgState init() {
			return new AvgState();
		}

		@Override
		public AvgState reduce(AvgState accumulator, Object input) {
			accumulator.count++;
			accumulator.total = accumulator.total + (Integer) input;
			return accumulator;
		}

		@Override
		public Object extractResult(AvgState accumulator) {
			if (accumulator.count > 0) {
				return accumulator.total / accumulator.count;
			}
			return null;
		}
	}

}
