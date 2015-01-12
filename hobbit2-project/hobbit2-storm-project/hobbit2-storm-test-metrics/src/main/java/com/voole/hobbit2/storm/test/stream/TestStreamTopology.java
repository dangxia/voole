package com.voole.hobbit2.storm.test.stream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;

import com.voole.hobbit2.storm.test.stream.StreamTuples.FirstTestStreamSpecial;
import com.voole.hobbit2.storm.test.stream.StreamTuples.SecondTestStreamSpecial;
import com.voole.hobbit2.storm.test.stream.StreamTuples.SpoutType;
import com.voole.hobbit2.storm.test.stream.StreamTuples.ThirdTestStreamSpecial;
import com.voole.hobbit2.storm.test.stream.TestStreamState.TestStreamStateFactory;
import com.voole.hobbit2.storm.test.stream.TestStreamState.TestStreamStateUpdater;

public class TestStreamTopology {
	public static Config getConfig() {
		Config conf = new Config();
		conf.setMaxSpoutPending(2);
		conf.setNumWorkers(4);
		conf.setMaxTaskParallelism(1);
		// conf.setDebug(true);
		conf.put(Config.TOPOLOGY_NAME, "test-stream-" + UUID.randomUUID());

		return conf;
	}

	public static TridentTopology createTopology() {
		TridentTopology topology = new TridentTopology();

		TestStreamSpout spout1 = new TestStreamSpout(
				new FirstTestStreamSpecial());

		TestStreamSpout spout2 = new TestStreamSpout(
				new SecondTestStreamSpecial());

		TestStreamSpout spout3 = new TestStreamSpout(
				new ThirdTestStreamSpecial());

		Stream stream1 = topology.newStream("first-spout", spout1)
				.parallelismHint(2).shuffle();
		Stream stream2 = topology.newStream("second-spout", spout2)
				.parallelismHint(2).shuffle()
				.project(new Fields("type", "num"));

		Stream stream3 = topology.newStream("second-spout", spout3)
				.parallelismHint(2).shuffle()
				.project(new Fields("type", "num"));

		// Stream mergeStream1 = topology.merge(stream1, stream2);

		TridentState state1 = stream1.partitionPersist(
				new TestStreamStateFactory("first_state"),
				StreamTuples.FIRST_OUT_FIELDS, new TestStreamStateUpdater(
						SpoutType.STREAM_STATE_1),
				StreamTuples.FIRST_OUT_FIELDS);
		TridentState state2 = stream2.partitionPersist(
				new TestStreamStateFactory("second_state"),
				StreamTuples.FIRST_OUT_FIELDS, new TestStreamStateUpdater(
						SpoutType.STREAM_STATE_1),
				StreamTuples.FIRST_OUT_FIELDS);
		// mergeStream1 = state1.newValuesStream();
		Stream mergeStream1 = topology.merge(state1.newValuesStream(),
				state2.newValuesStream());

		Stream mergeStream2 = topology.merge(mergeStream1, stream3);

		TridentState state3 = mergeStream2.partitionPersist(
				new TestStreamStateFactory("third_state"),
				StreamTuples.FIRST_OUT_FIELDS, new TestStreamStateUpdater(
						SpoutType.STREAM_STATE_1),
				StreamTuples.FIRST_OUT_FIELDS);

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
