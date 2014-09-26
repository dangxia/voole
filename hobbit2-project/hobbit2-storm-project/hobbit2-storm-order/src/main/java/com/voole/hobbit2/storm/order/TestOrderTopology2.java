/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;

import com.voole.hobbit2.storm.order.kryodecorator.StromOrderKryoDecorator;
import com.voole.hobbit2.storm.order.spout.OpaqueTridentKafkaSpout;
import com.voole.hobbit2.storm.order.state.SessionStateImpl.SessionStateFactory;
import com.voole.hobbit2.storm.order.state.SessionStateUpdate;

/**
 * @author XuehuiHe
 * @date 2014年9月26日
 */
public class TestOrderTopology2 {
	private static final Logger log = LoggerFactory
			.getLogger(TestOrderTopology.class);

	public static Config getConfig() {
		Config conf = new Config();
		conf.setMaxSpoutPending(1);
		conf.setNumWorkers(1);
		conf.setMaxTaskParallelism(1);
		conf.registerDecorator(StromOrderKryoDecorator.class);
		conf.put(Config.TOPOLOGY_NAME, "storm_order_" + UUID.randomUUID());

		return conf;
	}

	public static TridentTopology createTopology() {
		TridentTopology topology = new TridentTopology();
		OpaqueTridentKafkaSpout orderKafkaSpout = new OpaqueTridentKafkaSpout();
		Stream stream = topology.newStream("order-kafka-stream",
				orderKafkaSpout).parallelismHint(12);
		stream.shuffle()
				.partitionPersist(new SessionStateFactory(),
						new Fields("data"), new SessionStateUpdate())
				.parallelismHint(4);

		return topology;
	}

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException {

		TridentTopology topology = createTopology();
		Config conf = getConfig();

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology((String) conf.get(Config.TOPOLOGY_NAME), conf,
				topology.build());

	}
}
