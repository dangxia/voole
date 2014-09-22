/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order;

import java.util.Map;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.voole.hobbit2.storm.order.serializer.AvroKryoFactory;
import com.voole.hobbit2.storm.order.spout.OpaqueTridentKafkaSpout;

/**
 * @author XuehuiHe
 * @date 2014年9月22日
 */
public class TestOrderTopology {
	public static Config getConfig() {
		Config conf = new Config();
		conf.setMaxSpoutPending(20);
		conf.setNumWorkers(2);
		// conf.setMaxTaskParallelism(10);
		conf.setKryoFactory(AvroKryoFactory.class);

		return conf;
	}

	public static class Print extends BaseFilter {
		private Gson gson;

		@Override
		public void prepare(@SuppressWarnings("rawtypes") Map conf,
				TridentOperationContext context) {
			GsonBuilder gb = new GsonBuilder();
			gb.setPrettyPrinting();
			gson = gb.create();
			super.prepare(conf, context);
		}

		@Override
		public boolean isKeep(TridentTuple tuple) {
			System.out.println(gson.toJson(tuple));
			return true;
		}

	}

	public static TridentTopology createTopology() {
		TridentTopology topology = new TridentTopology();
		OpaqueTridentKafkaSpout orderKafkaSpout = new OpaqueTridentKafkaSpout();
		Stream stream = topology.newStream("order-kafka-stream",
				orderKafkaSpout).parallelismHint(4);
		stream.shuffle().each(new Fields("data"), new Print())
				.parallelismHint(1);
		return topology;
	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

		TridentTopology topology = createTopology();
		Config conf = getConfig();

		StormSubmitter.submitTopology(args[0], conf, topology.build());
		// LocalCluster cluster = new LocalCluster();
		// cluster.submitTopology("test-kafka-spout-name", conf,
		// topology.build());
	}
}
