/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.hive.order.mapreduce;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;

import com.voole.hobbit.hive.order.HiveOrderConfigs;

/**
 * @author XuehuiHe
 * @date 2014年7月29日
 */
public class HiveOrderInputFileFilter {
	private final long lastStamp;
	private final Pattern p = Pattern.compile("(\\d+)\\.\\w+$");
	private long maxStamp = 0l;

	public HiveOrderInputFileFilter(JobContext job) {
		lastStamp = HiveOrderConfigs.getPrevCamusMaxStamp(job);
	}

	public boolean accept(Path path) {
		String name = path.getName();
		if (!name.endsWith("avro")) {
			return false;
		}
		if (name.startsWith("noend")) {
			return true;
		}
		Matcher m = p.matcher(name);
		if (m.find()) {
			long stamp = Long.parseLong(m.group(1));
			if (stamp > lastStamp) {
				if (stamp > maxStamp) {
					maxStamp = stamp;
				}
				System.out.println(path.toUri().getPath());
				return true;
			}
		}
		return false;
	}

	public long getMaxStamp() {
		return maxStamp;
	}

	public void setMaxStamp(long maxStamp) {
		this.maxStamp = maxStamp;
	}

}
