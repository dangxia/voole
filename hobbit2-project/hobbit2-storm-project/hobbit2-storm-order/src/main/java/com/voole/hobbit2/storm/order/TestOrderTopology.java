/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import com.voole.hobbit2.storm.order.kryodecorator.StromOrderKryoDecorator;
import com.voole.hobbit2.storm.order.spout.OpaqueTridentKafkaSpout;

/**
 * @author XuehuiHe
 * @date 2014年9月22日
 */
public class TestOrderTopology {
	private static final Logger log = LoggerFactory
			.getLogger(TestOrderTopology.class);

	public static Config getConfig() {
		Config conf = new Config();
		conf.setMaxSpoutPending(20);
		conf.setNumWorkers(2);
		conf.registerDecorator(StromOrderKryoDecorator.class);
		conf.put(Config.TOPOLOGY_NAME, "storm_order_" + UUID.randomUUID());

		return conf;
	}

	public static class Print extends BaseFilter {
		private final Map<String, AtomicLong> total;
		private final AtomicLong arrayLen;

		public Print() {
			total = new HashMap<String, AtomicLong>();
			arrayLen = new AtomicLong(0l);
		}

		@Override
		public void prepare(@SuppressWarnings("rawtypes") Map conf,
				TridentOperationContext context) {
			super.prepare(conf, context);
		}

		@Override
		public boolean isKeep(TridentTuple tuple) {

			if (tuple.get(0).getClass().isArray()) {
				long result = arrayLen.incrementAndGet();
				if (result % 100000 == 0) {
					log.info(" array size:" + result);
				}
			} else {
				String name = tuple.get(0).getClass().toString();
				if (!total.containsKey(name)) {
					total.put(name, new AtomicLong(0l));
				}
				long result = total.get(name).incrementAndGet();
				if (result % 10000 == 0) {
					log.info(name + " size:" + result);
				}
			}

			return false;
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

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException {

		TridentTopology topology = createTopology();
		Config conf = getConfig();

		StormSubmitter.submitTopology((String) conf.get(Config.TOPOLOGY_NAME),
				conf, topology.build());
	}
}
