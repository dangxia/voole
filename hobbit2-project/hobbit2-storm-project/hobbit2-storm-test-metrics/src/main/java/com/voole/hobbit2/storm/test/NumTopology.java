package com.voole.hobbit2.storm.test;

import java.util.concurrent.TimeUnit;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

import com.voole.hobbit2.storm.test.NumBigBolt.NumSmallBolt;

public class NumTopology {
	public static void main(String[] args) throws InterruptedException,
			AlreadyAliveException, InvalidTopologyException {

		int maxSeconds = 15;
		int messageTimeOut = maxSeconds + 10;

		Config config = new Config();
		config.setMaxSpoutPending(5);
		config.setMessageTimeoutSecs(messageTimeOut);
		// config.setMaxTaskParallelism(1);
		config.setNumWorkers(4);
		// config.setNumAckers(1);
		config.setDebug(true);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("num_spout", new NumSpout(), 2);
		builder.setBolt("num_bolt", new NumBolt(), 2).shuffleGrouping(
				"num_spout");

		builder.setBolt("big", new NumBigBolt(), 2).directGrouping("num_bolt");
		builder.setBolt("small", new NumSmallBolt(), 2).directGrouping(
				"num_bolt");
		LocalCluster local = new LocalCluster();
		local.submitTopology("test-metrics", config, builder.createTopology());

		TimeUnit.SECONDS.sleep(100);
		local.killTopology("test-metrics");
		local.shutdown();

		// StormSubmitter.submitTopology("test-metrics", config,
		// builder.createTopology());

	}
}
