/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.hive.order.mapreduce;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voole.hobbit2.hive.order.HiveOrderMetaConfigs;

/**
 * @author XuehuiHe
 * @date 2014年7月29日
 */
public class HiveOrderInputFileFilter {
	private Logger log = LoggerFactory
			.getLogger(HiveOrderInputFileFilter.class);
	private final long prevCamusExecTime;
	private final Pattern p = Pattern.compile("(\\d+)\\.\\w+$");
	private long currCamusExecTime;
	private String camusDestPath;

	public HiveOrderInputFileFilter(JobContext job) {
		prevCamusExecTime = HiveOrderMetaConfigs.getPrevCamusExecTime(job);
		camusDestPath = HiveOrderMetaConfigs.getCamusDestPath(job).toUri()
				.getPath();
		currCamusExecTime = prevCamusExecTime;
	}

	public boolean accept(Path path) {
		String name = path.getName();
		if (isInCamsuDescPath(path)) {
			Matcher m = p.matcher(name);
			if (m.find()) {
				long stamp = Long.parseLong(m.group(1));
				if (stamp > prevCamusExecTime) {
					if (stamp > currCamusExecTime) {
						currCamusExecTime = stamp;
					}
					log.info("find camus desc file:" + path.toUri().getPath());
					return true;
				}
			} else {
				log.warn(path.toUri().getPath() + " not found exectime");
			}
		} else {
			if (name.startsWith(HiveOrderMetaConfigs.NOEND_PREFIX)) {
				log.info("find noedn file:" + path.toUri().getPath());
				return true;
			}
		}
		return false;
	}

	private boolean isInCamsuDescPath(Path path) {
		return path.toUri().getPath().startsWith(camusDestPath);
	}

	public long getCurrCamusExecTime() {
		return currCamusExecTime;
	}

	public void setCurrCamusExecTime(long currCamusExecTime) {
		this.currCamusExecTime = currCamusExecTime;
	}

}
