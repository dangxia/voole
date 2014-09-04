/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.mr.partitioner;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author XuehuiHe
 * @date 2014年9月4日
 */
public class HourlyPartitioner implements ICamusPartitioner {
	private static final SimpleDateFormat df = new SimpleDateFormat(
			"yyyy/MM/dd/HH/");

	@Override
	public String getPath(long partitionStamp) {
		return "/hourly/" + df.format(new Date(partitionStamp));
	}

	@Override
	public long getPartitionStamp(long stamp) {
		return stamp / (60 * 60 * 1000) * 60 * 60 * 1000;
	}

}
