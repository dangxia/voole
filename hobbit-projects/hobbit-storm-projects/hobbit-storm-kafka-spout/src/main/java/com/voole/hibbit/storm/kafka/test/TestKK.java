/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hibbit.storm.kafka.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import kafka.api.OffsetRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;

import com.voole.hibbit.storm.kafka.OpaqueTridentKafkaSpout;
import com.voole.hibbit.storm.kafka.partition.BrokerHosts.ZkHosts;
import com.voole.hibbit.storm.kafka.partition.KafkaConfig;

/**
 * @author XuehuiHe
 * @date 2014年5月29日
 */
public class TestKK {
	private static Logger logger = LoggerFactory.getLogger(TestKK.class);

	/**
	 * @author XuehuiHe
	 * @date 2014年5月29日
	 */
	public static final class TestKafkaFunction extends BaseFunction {
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			try {
				TimeUnit.MILLISECONDS.sleep(100);
				byte[] bytes = tuple.getBinaryByField("bytes");
				logger.info("msg:" + new String(bytes));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			// String topic = tuple.getStringByField("topic");
			// long offset = tuple.getLongByField("offset");
			// byte[] bytes = tuple.getBinaryByField("bytes");
			// int partition = tuple.getIntegerByField("partition");
			// logger.info("topic:" + topic + "\tpartition:" + partition
			// + "\toffset:" + offset + "\tmsg:" + new String(bytes));
		}
	}

	public static void main_local(String[] args) throws InterruptedException {
		ZkHosts hosts = new ZkHosts();
		hosts.setKafkaConnetion("data-zk1.voole.com:2181,data-zk2.voole.com:2181,data-zk3.voole.com:2181/kafka");
		KafkaConfig kafkaConfig = new KafkaConfig(hosts, "t_playalive_v3");
		kafkaConfig.forceStartOffsetTime(OffsetRequest.EarliestTime());
		kafkaConfig.setFetchSizeBytes(1000);

		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(kafkaConfig);
		TridentTopology topology = new TridentTopology();
		topology.newStream("test-kafka-spout", spout)
				.parallelismHint(2)
				.partition(new LocalShuffle())
				.each(new Fields("bytes"), new TestKafkaFunction(),
						new Fields()).parallelismHint(4);
		// .parallelismHint(5);
		Config conf = new Config();
		conf.setMaxSpoutPending(2);
		conf.setNumWorkers(2);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test-kafka-spout-name", conf, topology.build());

		TimeUnit.SECONDS.sleep(10000);
		cluster.killTopology("test-kafka-spout-name");
		cluster.shutdown();
	}

	public static class LocalShuffle implements CustomStreamGrouping {
		private List<Integer> targetTasks;
		private int size;
		private Random r;

		@Override
		public void prepare(WorkerTopologyContext context,
				GlobalStreamId stream, List<Integer> targetTasks) {
			this.targetTasks = new ArrayList<Integer>();
			Set<Integer> localTasks = new HashSet<Integer>(
					context.getThisWorkerTasks());
			for (Integer taskId : targetTasks) {
				if (localTasks.contains(taskId)) {
					this.targetTasks.add(taskId);
				}
			}
			if (this.targetTasks.size() == 0) {
				this.targetTasks.addAll(targetTasks);
			}
			this.size = this.targetTasks.size();

			r = new Random();
		}

		@Override
		public List<Integer> chooseTasks(int taskId, List<Object> values) {
			int index = r.nextInt(size);
			return Arrays.asList(targetTasks.get(index));
		}

	}

	public static void main(String[] args) throws InterruptedException {
		main_local(args);
	}

	public static void main_cluster(String[] args) throws InterruptedException,
			AlreadyAliveException, InvalidTopologyException {
		ZkHosts hosts = new ZkHosts();
		hosts.setKafkaConnetion("data-zk1.voole.com:2181,data-zk2.voole.com:2181,data-zk3.voole.com:2181/kafka");
		KafkaConfig kafkaConfig = new KafkaConfig(hosts, "t_playalive_v3");
		// kafkaConfig.forceStartOffsetTime(OffsetRequest.EarliestTime());

		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(kafkaConfig);
		TridentTopology topology = new TridentTopology();
		topology.newStream("test-kafka-spout", spout)
				.groupBy(new Fields("partition"))
				.each(new Fields("topic", "partition", "offset", "bytes"),
						new TestKafkaFunction(), new Fields());
		// .parallelismHint(10);

		Config conf = new Config();
		conf.setMaxSpoutPending(20);

		StormSubmitter.submitTopology("test-kafka-spout-name", conf,
				topology.build());

		// LocalCluster cluster = new LocalCluster();
		// cluster.submitTopology("test-kafka-spout-name", conf,
		// topology.build());
		//
		// TimeUnit.SECONDS.sleep(10000);
		// cluster.killTopology("test-kafka-spout-name");
		// cluster.shutdown();
	}
}
