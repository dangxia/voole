package com.voole.hobbit2.storm.test;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class NumBigBolt extends BaseRichBolt {

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {

	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	public static class NumSmallBolt extends BaseRichBolt {

		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void execute(Tuple input) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			
		}

	}

}
