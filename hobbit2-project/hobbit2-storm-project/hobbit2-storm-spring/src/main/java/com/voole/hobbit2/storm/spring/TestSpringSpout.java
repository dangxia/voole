/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.spring;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import storm.trident.TridentTopology;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.voole.hobbit2.storm.spring.SessionStateImpl.SessionStateFactory;

/**
 * @author XuehuiHe
 * @date 2014年9月26日
 */
public class TestSpringSpout implements IBatchSpout {

	public static Config createConfig() {
		Config config = new Config();
		config.setMaxSpoutPending(2);
		config.setNumWorkers(2);
		// config.setDebug(true);
		return config;
	}

	public static void main(String[] args) throws InterruptedException,
			AlreadyAliveException, InvalidTopologyException {
		Config config = createConfig();
		TestSpringSpout spout = new TestSpringSpout();

		TridentTopology topology = new TridentTopology();
		topology.newStream("kkkk", spout)
				.shuffle()
				.partitionPersist(new SessionStateFactory(),
						new Fields("word"), new SessionStateUpdate())
				.parallelismHint(4);
		StormSubmitter.submitTopology("kkkkkk", config, topology.build());
//		LocalCluster cluster = new LocalCluster();
//		cluster.submitTopology("kkkkkk", config, topology.build());

	}

	private static final Fields OUT_FIELDS = new Fields("word");
	private AtomicBoolean sended;
	private AtomicBoolean sended2;
	private AtomicBoolean sended3;

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context) {
		sended = new AtomicBoolean(false);
		sended2 = new AtomicBoolean(false);
		sended3 = new AtomicBoolean(false);
	}

	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		if (sended.compareAndSet(false, true)) {
			List<String> source = new ArrayList<String>();
			source.add("one");
			source.add("two");
			source.add("one");
			for (String item : source) {
				collector.emit(new Values(item));
			}
		} else if (sended2.compareAndSet(false, true)) {
			List<String> source = new ArrayList<String>();
			source.add("two");
			source.add("three");
			for (String item : source) {
				collector.emit(new Values(item));
			}
		} else if (sended3.compareAndSet(false, true)) {
			List<String> source = new ArrayList<String>();
			source.add("one");
			source.add("two");
			for (String item : source) {
				collector.emit(new Values(item));
			}
		}
	}

	@Override
	public void ack(long batchId) {

	}

	@Override
	public void close() {

	}

	@SuppressWarnings("rawtypes")
	@Override
	public Map getComponentConfiguration() {
		return null;
	}

	@Override
	public Fields getOutputFields() {
		return OUT_FIELDS;
	}

}
