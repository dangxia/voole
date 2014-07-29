/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.hive.order.mapreduce;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * @author XuehuiHe
 * @date 2014年7月29日
 */
public class HiveOrderInputFileFilter {
	private final long lastStamp;
	private final Pattern p = Pattern.compile("(\\d+)\\.\\w+$");

	public HiveOrderInputFileFilter(JobContext job) {
		lastStamp = 1406617057641l;
	}

	public boolean accept(Path path) {
		String url = path.toUri().getPath();
		Matcher m = p.matcher(url);
		if (m.find()) {
			long stamp = Long.parseLong(m.group(1));
			return stamp > lastStamp;
		}
		return false;
	}

}
