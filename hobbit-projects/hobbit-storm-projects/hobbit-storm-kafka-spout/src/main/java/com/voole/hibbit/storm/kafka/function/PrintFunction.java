/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hibbit.storm.kafka.function;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 * @author XuehuiHe
 * @date 2014年6月4日
 */
public class PrintFunction extends BaseFilter {

	@Override
	public boolean isKeep(TridentTuple tuple) {
		System.out.println(tuple);
		return true;
	}

}
