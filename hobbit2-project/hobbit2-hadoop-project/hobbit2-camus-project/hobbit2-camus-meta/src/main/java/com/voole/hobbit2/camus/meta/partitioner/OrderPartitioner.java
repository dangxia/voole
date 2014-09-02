/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta.partitioner;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.avro.specific.SpecificRecordBase;

import com.voole.hobbit2.camus.meta.common.CamusKey;
import com.voole.hobbit2.kafka.avro.order.util.OrderStampFinder;
import com.voole.hobbit2.kafka.common.partition.Partitioner;

/**
 * @author XuehuiHe
 * @date 2014年8月29日
 */
public class OrderPartitioner implements
		Partitioner<CamusKey, SpecificRecordBase> {
	private static SimpleDateFormat df = new SimpleDateFormat("/yyyy/MM/dd/HH/");

	@Override
	public void partition(CamusKey key, SpecificRecordBase value) {
		key.setStamp(getCategoryTime(value));
	}

	@Override
	public String getPath(CamusKey key) {
		return key.getTopic() + "/hourly" + df.format(new Date(key.getStamp()));
	}

	private long getCategoryTime(SpecificRecordBase value) {
		long stamp = OrderStampFinder.getStamp(value);
		stamp = stamp / (60 * 60 * 1000) * 60 * 60 * 1000;
		return stamp;
	}

}
