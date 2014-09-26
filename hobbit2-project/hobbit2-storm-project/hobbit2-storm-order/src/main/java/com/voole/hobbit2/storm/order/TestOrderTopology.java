/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order;

import java.util.UUID;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;

import com.voole.hobbit2.storm.order.kryodecorator.StromOrderKryoDecorator;
import com.voole.hobbit2.storm.order.spout.OpaqueTridentKafkaSpout;
import com.voole.hobbit2.storm.order.state.ExtraInfoQueryStateFunction;
import com.voole.hobbit2.storm.order.state.ExtraInfoQueryStateImpl.ExtraInfoQueryStateFactory;
import com.voole.hobbit2.storm.order.state.SessionStateImpl.SessionStateFactory;
import com.voole.hobbit2.storm.order.state.SessionStateUpdate;

/**
 * @author XuehuiHe
 * @date 2014年9月22日
 */
public class TestOrderTopology {

	public static Config getConfig() {
		Config conf = new Config();
		conf.setMaxSpoutPending(4);
		conf.setNumWorkers(12);
		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 10 * 60);
		conf.registerDecorator(StromOrderKryoDecorator.class);
		conf.put(Config.TOPOLOGY_NAME, "storm_order_" + UUID.randomUUID());

		return conf;
	}

	public static TridentTopology createTopology() {
		TridentTopology topology = new TridentTopology();
		TridentState queryState = topology.newStaticState(
				new ExtraInfoQueryStateFactory()).parallelismHint(2);
		OpaqueTridentKafkaSpout orderKafkaSpout = new OpaqueTridentKafkaSpout();
		Stream stream = topology
				.newStream("order-kafka-stream", orderKafkaSpout)
				.parallelismHint(24).shuffle();
		stream = ExtraInfoQueryStateFunction.query(stream, queryState);
		stream.shuffle()
				.partitionPersist(new SessionStateFactory(),
						new Fields("dry"), new SessionStateUpdate())
				.parallelismHint(10);

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
