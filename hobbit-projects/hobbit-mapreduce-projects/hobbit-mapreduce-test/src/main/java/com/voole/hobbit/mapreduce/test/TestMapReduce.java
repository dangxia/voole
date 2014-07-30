/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.mapreduce.test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author XuehuiHe
 * @date 2014年7月30日
 */
public class TestMapReduce extends Configured implements Tool {
	private final SimpleDateFormat df = new SimpleDateFormat(
			"yyyy-MM-dd-HH-mm-ss");

	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "root");
		ToolRunner.run(new TestMapReduce(), args);
	}

	public static class TestMapper extends
			Mapper<LongWritable, Text, Text, LongWritable> {
		private LongWritable l = new LongWritable(1);
		private Text t = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] strs = value.toString().split("[\\s\\n\\r\\t]");
			for (String string : strs) {
				if (string.length() > 0) {
					t.set(string);
					context.write(t, l);
				}
			}
		}
	}

	public static class TestReducer extends
			Reducer<Text, LongWritable, Text, NullWritable> {
		private Text t = new Text();

		@Override
		protected void reduce(Text key, Iterable<LongWritable> iterable,
				Context context) throws IOException, InterruptedException {
			long count = 0;
			for (LongWritable longWritable : iterable) {
				count += longWritable.get();
			}
			t.set(key.toString() + "_" + count);
			context.write(t, NullWritable.get());
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJarByClass(TestMapReduce.class);
		job.setJobName("test_map_reduce");

		FileInputFormat.setInputPaths(job, "/tmp/hexh/simple-word-count/input");
		FileOutputFormat.setOutputPath(job,
				new Path("/tmp/hexh/" + df.format(new Date())));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setMapperClass(TestMapper.class);
		job.setReducerClass(TestReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.submit();
		job.waitForCompletion(true);
		return 0;
	}

}
