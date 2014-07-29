/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.hive.order.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * @author XuehuiHe
 * @date 2014年7月29日
 */
public class HiveOrderInputPathFilter implements PathFilter {
	@Override
	public boolean accept(Path path) {
		// TODO
		return false;
	}

}
