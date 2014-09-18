/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.partition;

import java.io.FileNotFoundException;
import java.util.List;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;

import com.voole.hobbit2.common.Hobbit2Configuration;

import storm.trident.spout.ISpoutPartition;

/**
 * @author XuehuiHe
 * @date 2014年9月18日
 */
public class StormOrderSpoutPartitionCreator {
	public static List<ISpoutPartition> create() throws ConfigurationException, FileNotFoundException {
		CompositeConfiguration hobbit2Configuration = Hobbit2Configuration
				.initConfig();
		// TODO
		return null;
	}
}
