/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.order.record.mapreduce;

import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.voole.hobbit.transformer.KafkaTerminalAvroTransformer;

/**
 * @author XuehuiHe
 * @date 2014年7月28日
 */
public class MapReduceColorCount extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "root");
		Job job = Job.getInstance(getConf());
		job.setJarByClass(MapReduceColorCount.class);
		job.setJobName("test avro map reduce");

		FileInputFormat.setInputPaths(job, new Path(
				"/kafka/t_playalive_v2/hourly/2014/07/21/17"));
		job.setInputFormatClass(AvroKeyInputFormat.class);
		AvroJob.setInputKeySchema(job, KafkaTerminalAvroTransformer
				.getKafkaTopicSchema("t_playalive_v2"));
		job.setNumReduceTasks(0);

		FileOutputFormat.setOutputPath(job, new Path("/tmp/test_order_record2"));
		job.setOutputFormatClass(AvroKeyOutputFormat.class);
		AvroJob.setOutputKeySchema(job, KafkaTerminalAvroTransformer
				.getKafkaTopicSchema("t_playalive_v2"));
		// AvroJob.setOutputValueSchema(job, KafkaTerminalAvroTransformer
		// .getKafkaTopicSchema("t_playalive_v2"));
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new MapReduceColorCount(), args);
	}

}
