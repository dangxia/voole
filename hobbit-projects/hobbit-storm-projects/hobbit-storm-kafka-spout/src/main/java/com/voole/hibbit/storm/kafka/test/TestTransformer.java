/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hibbit.storm.kafka.test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import kafka.api.OffsetRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;

import com.voole.hibbit.storm.kafka.OpaqueTridentKafkaSpout;
import com.voole.hibbit.storm.kafka.partition.BrokerHosts.ZkHosts;
import com.voole.hibbit.storm.kafka.partition.KafkaConfig;
import com.voole.hobbit.proto.TerminalPB.OrderPlayAliveReqV3;
import com.voole.hobbit.transformer.KafkaTerminalProtoBuffTransformer;

/**
 * @author XuehuiHe
 * @date 2014年5月30日
 */
public class TestTransformer {
	private static Logger logger = LoggerFactory
			.getLogger(TestTransformer.class);

	/**
	 * @author XuehuiHe
	 * @date 2014年5月29日
	 */
	public static final class TestKafkaFunction extends BaseFunction {
		private KafkaTerminalProtoBuffTransformer<OrderPlayAliveReqV3> transformer;

		@Override
		public void prepare(Map conf, TridentOperationContext context) {
			super.prepare(conf, context);
			try {
				transformer = new KafkaTerminalProtoBuffTransformer<OrderPlayAliveReqV3>(
						OrderPlayAliveReqV3.class);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			byte[] bytes = tuple.getBinaryByField("bytes");
			long offset = tuple.getLongByField("offset");
			try {
				if (offset % 10000 == 0) {
					int partition = tuple.getIntegerByField("partition");
					logger.info("partition:" + partition + "\toffset:" + offset);
				}
				transformer.transform(bytes);
			} catch (RuntimeException e) {
				logger.info(e.getMessage());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main_cluster(String[] args) throws InterruptedException,
			AlreadyAliveException, InvalidTopologyException {
		ZkHosts hosts = new ZkHosts();
		hosts.setKafkaConnetion("data-zk1.voole.com:2181,data-zk2.voole.com:2181,data-zk3.voole.com:2181/kafka");
		KafkaConfig kafkaConfig = new KafkaConfig(hosts, "t_playalive_v3");
		kafkaConfig.forceStartOffsetTime(OffsetRequest.EarliestTime());

		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(kafkaConfig);
		TridentTopology topology = new TridentTopology();
		topology.newStream("test-kafka-spout", spout)
				.parallelismHint(4)
				.shuffle()
				.each(new Fields("offset", "partition", "bytes"),
						new TestKafkaFunction(), new Fields())
				.parallelismHint(16);

		Config conf = new Config();
		conf.setMaxSpoutPending(20);
		conf.setNumWorkers(4);

		StormSubmitter.submitTopology("test-kafka-spout-name", conf,
				topology.build());
	}

	public static void main_local(String[] args) throws InterruptedException {
		ZkHosts hosts = new ZkHosts();
		hosts.setKafkaConnetion("data-zk1.voole.com:2181,data-zk2.voole.com:2181,data-zk3.voole.com:2181/kafka");
		KafkaConfig kafkaConfig = new KafkaConfig(hosts, "t_playalive_v3");
		kafkaConfig.forceStartOffsetTime(OffsetRequest.EarliestTime());
		kafkaConfig.setFetchSizeBytes(10000);

		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(kafkaConfig);
		TridentTopology topology = new TridentTopology();
		topology.newStream("test-kafka-spout", spout)
		// .partitionBy(new Fields("partition"))
		// .groupBy(new Fields("partition"))
				.each(new Fields("bytes"), new TestKafkaFunction(),
						new Fields()).parallelismHint(12);
		Config conf = new Config();
		conf.setMaxSpoutPending(40);
		conf.setNumWorkers(3);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test-kafka-spout-name", conf, topology.build());

		// TimeUnit.SECONDS.sleep(10000);
		// cluster.killTopology("test-kafka-spout-name");
		// cluster.shutdown();
	}

	public static void main(String[] args) throws InterruptedException,
			AlreadyAliveException, InvalidTopologyException {
		main_cluster(args);
	}
}
