/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.mapreduce.test;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

/**
 * @author XuehuiHe
 * @date 2014年7月30日
 */
public class TestMapReduce extends Configured implements Tool {

	
	public static class TestMapper extends Mapper<NullWritable, Text, NullWritable, VALUEOUT>
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

}
