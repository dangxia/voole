package com.voole.hobbit2.storm.order2.config;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.voole.hobbit2.storm.order2.config.TopologyConfigs.SyncStartFrom;

public class TestTopologyConfigs {
	@Test
	public void testSyncStartFrom() {
		for (SyncStartFrom from : SyncStartFrom.values()) {
			testSyncStartFrom(from);
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSyncStartFromException() {
		TopologyConfigs.setSyncStartFrom(null, "wrong-from");
	}

	@SuppressWarnings("rawtypes")
	public void testSyncStartFrom(SyncStartFrom from) {
		Map config = new HashMap();
		TopologyConfigs.setSyncStartFrom(config, from.getConfig());
		Assert.assertEquals(from, TopologyConfigs.getSyncStartFrom(config));
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testSyncHdfsNoendPoint() {
		Map config = new HashMap();
		Calendar c = Calendar.getInstance();
		c.set(Calendar.MILLISECOND, 0);
		Date point = c.getTime();
		TopologyConfigs.setSyncHdfsNoendPoint(config,
				TopologyConfigs.df.format(point));
		Assert.assertEquals(point,
				TopologyConfigs.getSyncHdfsNoendPoint(config));

		config = new HashMap();
		TopologyConfigs.setSyncHdfsNoendPoint(config, null);
		Assert.assertNull(TopologyConfigs.getSyncHdfsNoendPoint(config));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSyncHdfsNoendPoint2() {
		TopologyConfigs.setSyncHdfsNoendPoint(null, "wrong-date-format");
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void testWhiteTopics() {
		Map config = new HashMap();
		List<String> topics = Lists.newArrayList("topic1", "topic2", "topic3");
		TopologyConfigs.setWhiteTopics(config, Joiner.on(',').join(topics));
		Assert.assertEquals(topics, TopologyConfigs.getWhiteTopics(config));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testWhiteTopics2() {
		TopologyConfigs.setWhiteTopics(null, "");
	}
}
