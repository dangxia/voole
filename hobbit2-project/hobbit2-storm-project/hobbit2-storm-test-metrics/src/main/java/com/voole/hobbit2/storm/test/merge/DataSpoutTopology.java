package com.voole.hobbit2.storm.test.merge;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;

import com.voole.hobbit2.storm.test.merge.DataSpout.DataSpoutStateUpdater;
import com.voole.hobbit2.storm.test.merge.SpoutTuples.SpoutCustomStreamGrouping;

public class DataSpoutTopology {
	public static Config getConfig() {
		Config conf = new Config();
		conf.setMaxSpoutPending(2);
		conf.setNumWorkers(4);
		// conf.setDebug(true);
		conf.put(Config.TOPOLOGY_NAME, "data-spout" + UUID.randomUUID());

		return conf;
	}

	public static TridentTopology createTopology() {
		TridentTopology topology = new TridentTopology();

		DataSpout spout = new DataSpout(10l);
		DataSpout spout2 = new DataSpout(3l);
		Stream stream = topology.newStream("data-spout", spout)
				.parallelismHint(2).shuffle();
		Stream stream2 = topology.newStream("data-spout2", spout2)
				.parallelismHint(2).shuffle();

		stream = topology.merge(stream, stream2);

		stream = stream.partition(new SpoutCustomStreamGrouping());
		stream = stream
				.partitionPersist(
						new com.voole.hobbit2.storm.test.merge.DataSpout.DataSpoutStateFactory(),
						SpoutTuples.SPOUT_OUT_FIELDS,
						new DataSpoutStateUpdater()).parallelismHint(1)
				.newValuesStream();

		// stream.partition(partitioner)

		// stream.persistentAggregate(
		// new
		// com.voole.hobbit2.storm.test.merge.DataSpout.DataSpoutStateFactory(),
		// new Fields("num"),
		// new
		// com.voole.hobbit2.storm.test.merge.DataSpout.DataSpoutCombinerAggregator(),
		// new Fields("num2"));

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
}
