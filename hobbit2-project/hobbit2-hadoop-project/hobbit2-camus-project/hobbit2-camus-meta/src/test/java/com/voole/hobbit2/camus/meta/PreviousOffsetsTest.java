/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Optional;
import com.voole.hobbit2.tools.kafka.partition.TopicPartition;

/**
 * @author XuehuiHe
 * @date 2014年8月27日
 */
public class PreviousOffsetsTest {
	private Map<TopicPartition, Long> prevOffsets;
	private Configuration conf;
	private Path path;
	private FileSystem fs;

	@Before
	public void before() throws IOException {
		System.setProperty("HADOOP_USER_NAME", "root");
		conf = new Configuration();
		fs = FileSystem.get(conf);
		path = new Path("/tmp/PreviousOffsetsTest.txt");

		clean();

		prevOffsets = new HashMap<TopicPartition, Long>();
		prevOffsets.put(new TopicPartition("test1", 2), 3l);
		prevOffsets.put(new TopicPartition("test1", 2), 4l);
		prevOffsets.put(new TopicPartition("test1", 3), 5l);
	}

	@Test
	public void test() throws IOException {
		CamusHDFSUtils.writePreviousOffsets(conf, path, prevOffsets);

		Map<TopicPartition, Long> result = CamusHDFSUtils.readPreviousOffsets(
				conf, Optional.of(path.getParent()), new PathFilter() {
					@Override
					public boolean accept(Path path) {
						return path.getName().equals("PreviousOffsetsTest.txt");
					}
				});
		Assert.assertEquals(2, result.size());
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
