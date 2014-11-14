/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;

import com.google.common.base.Optional;
import com.voole.hobbit2.storm.order.kryodecorator.StromOrderKryoDecorator;
import com.voole.hobbit2.storm.order.spout.OpaqueTridentKafkaSpout;
import com.voole.hobbit2.storm.order.state.ExtraInfoQueryStateFunction;
import com.voole.hobbit2.storm.order.state.ExtraInfoQueryStateImpl.ExtraInfoQueryStateFactory;
import com.voole.hobbit2.storm.order.state.SessionStateImpl.SessionStateFactory;
import com.voole.hobbit2.storm.order.state.SessionStateUpdate;
import com.voole.hobbit2.storm.order.util.NoendFileProcessor;

/**
 * @author XuehuiHe
 * @date 2014年9月22日
 */
public class TestOrderTopology {
	private final static Logger log = LoggerFactory
			.getLogger(TestOrderTopology.class);

	public static Config getConfig() {
		Config conf = new Config();
		conf.setMaxSpoutPending(4);
		conf.setNumWorkers(14);
		// conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 10 * 60);
		conf.registerDecorator(StromOrderKryoDecorator.class);
		conf.put(Config.TOPOLOGY_NAME, "storm_order_" + UUID.randomUUID());

		return conf;
	}

	public static TridentTopology createTopology() {
		TridentTopology topology = new TridentTopology();
		TridentState queryState = topology.newStaticState(
				new ExtraInfoQueryStateFactory()).parallelismHint(1);
		OpaqueTridentKafkaSpout orderKafkaSpout = new OpaqueTridentKafkaSpout();
		Stream stream = topology
				.newStream("order-kafka-stream", orderKafkaSpout)
				.parallelismHint(28).shuffle();
		stream = ExtraInfoQueryStateFunction.query(stream, queryState);
		stream.shuffle()
				.partitionPersist(new SessionStateFactory(), new Fields("dry"),
						new SessionStateUpdate()).parallelismHint(4);

		return topology;
	}

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, FileNotFoundException, IOException, InterruptedException {
		Config conf = getConfig();
		Optional<Path> hiveOrderLastExecPath = StormOrderHDFSUtils
				.findHiveOrderLastExecPath();
		if (hiveOrderLastExecPath.isPresent()) {
			StormOrderMetaConfigs.setHiveOrderLastExecPath(conf,
					hiveOrderLastExecPath.get().toUri().getPath());
		}

		NoendFileProcessor.processNoendFiles(conf);

		TridentTopology topology = createTopology();
		StormSubmitter.submitTopology((String) conf.get(Config.TOPOLOGY_NAME),
				conf, topology.build());
	}
}
