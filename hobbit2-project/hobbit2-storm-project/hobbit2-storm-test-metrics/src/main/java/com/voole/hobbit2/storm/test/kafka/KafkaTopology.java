package com.voole.hobbit2.storm.test.kafka;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;

import com.voole.hobbit2.storm.test.kafka.avro.PlayType;

public class KafkaTopology {
	public static Config getConfig() {
		Config conf = new Config();
		conf.setMaxSpoutPending(2);
		conf.setNumWorkers(4);
		conf.registerDecorator(KafkaAvroKryoDecorator.class);
		// conf.setDebug(true);
		conf.put(Config.TOPOLOGY_NAME, "kafka-test-" + UUID.randomUUID());

		return conf;
	}

	public static TridentTopology createTopology() {
		TridentTopology topology = new TridentTopology();

		KafkaSpout kafkaSpout = new KafkaSpout();
		Stream stream = topology.newStream("kafka-spout-", kafkaSpout)
				.parallelismHint(3);

		stream.each(new Fields("type"), new Filter() {

			@Override
			public void prepare(Map conf, TridentOperationContext context) {
			}

			@Override
			public void cleanup() {
			}

			@Override
			public boolean isKeep(TridentTuple tuple) {
				PlayType type = (PlayType) tuple.get(0);
				if (type == PlayType.PARTITIONINFO) {
					return true;
				}
				return false;
			}
		}).shuffle().groupBy(new Fields("type"))
				.aggregate(new One(), new Fields("one"))
				.each(new Fields("type", "one"), new Filter() {

					@Override
					public void prepare(Map conf,
							TridentOperationContext context) {
					}

					@Override
					public void cleanup() {
					}

					@Override
					public boolean isKeep(TridentTuple tuple) {
						System.out.println("group type:" + tuple.get(0)
								+ ",count:" + tuple.get(1));
						return false;
					}
				});

		stream.each(new Fields("type"), new Filter() {

			@Override
			public void prepare(Map conf, TridentOperationContext context) {
			}

			@Override
			public void cleanup() {
			}

			@Override
			public boolean isKeep(TridentTuple tuple) {
				PlayType type = (PlayType) tuple.get(0);
				if (type != PlayType.PARTITIONINFO) {
					return true;
				}
				return false;
			}
		}).shuffle().each(new Fields("type"), new Filter() {

			@Override
			public void prepare(Map conf, TridentOperationContext context) {
				// TODO Auto-generated method stub

			}

			@Override
			public void cleanup() {
				// TODO Auto-generated method stub

			}

			@Override
			public boolean isKeep(TridentTuple tuple) {
				System.out.println(tuple.get(0));
				// TODO Auto-generated method stub
				return false;
			}
		}).parallelismHint(2);

		return topology;
	}

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, FileNotFoundException, IOException,
			InterruptedException {
		TridentTopology topology = createTopology();
		Config conf = getConfig();

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology((String) conf.get(Config.TOPOLOGY_NAME), conf,
				topology.build());
	}

	public static class One implements CombinerAggregator<Integer> {
		public Integer init(TridentTuple tuple) {
			return 1;
		}

		public Integer combine(Integer val1, Integer val2) {
			return val1 + val2;
		}

		public Integer zero() {
			return 0;
		}
	}

}
