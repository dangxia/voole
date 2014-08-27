/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import junit.framework.Assert;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Optional;
import com.voole.hobbit2.tools.kafka.partition.Broker;
import com.voole.hobbit2.tools.kafka.partition.BrokerAndTopicPartition;
import com.voole.hobbit2.tools.kafka.partition.PartitionState;
import com.voole.hobbit2.tools.kafka.partition.TopicPartition;

/**
 * @author XuehuiHe
 * @date 2014年8月27日
 */
public class PartionsStatesTest {
	private Map<TopicPartition, Long> prevOffsets;
	private List<PartitionState> states;
	private Configuration conf;
	private Path path;
	private FileSystem fs;

	@Before
	public void before() throws IOException {
		System.setProperty("HADOOP_USER_NAME", "root");
		conf = new Configuration();
		fs = FileSystem.get(conf);
		path = new Path("/tmp/PartionsStatesTest.txt");
		states = new ArrayList<PartitionState>();
		prevOffsets = new HashMap<TopicPartition, Long>();
		clean();

		for (int i = 0; i < 10; i++) {
			createData();
		}
	}

	private void createData() {
		Random r = new Random();

		PartitionState state = new PartitionState();
		state.setBrokerAndTopicPartition(new BrokerAndTopicPartition(new Broker(
				RandomStringUtils.random(5), r.nextInt(10000), r.nextInt(10)),
				new TopicPartition(RandomStringUtils.random(5), r.nextInt(10))));
		state.setEarliestOffset(r.nextInt(1000000));
		state.setLatestOffset(r.nextInt(1000000));
		state.setOffset(r.nextInt(1000000));

		states.add(state);
		prevOffsets.put(state.getBrokerAndTopicPartition().getPartition(), state.getOffset());

	}

	@Test
	public void test() throws IOException {
		CamusHDFSUtils.writePrevPartionsStates(conf, path, states);

		Map<TopicPartition, Long> result = CamusHDFSUtils
				.readPrevPartionsStates(conf, Optional.of(path.getParent()),
						"PartionsStatesTest.txt");
		Assert.assertEquals(prevOffsets.size(), result.size());

		for (Entry<TopicPartition, Long> entry : prevOffsets.entrySet()) {
			Assert.assertTrue(result.keySet().contains(entry.getKey()));
			Assert.assertEquals(entry.getValue(), result.get(entry.getKey()));
		}

	}

	public void clean() throws IOException {
		if (fs.exists(path.getParent())) {
			fs.mkdirs(path.getParent());
		}
		fs.deleteOnExit(path);
	}
}
