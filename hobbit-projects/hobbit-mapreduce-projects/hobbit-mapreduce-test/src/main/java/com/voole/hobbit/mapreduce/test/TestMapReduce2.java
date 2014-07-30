package com.voole.hobbit.mapreduce.test;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.voole.hobbit.avro.termial.OrderPlayBgnReqV2;

public class TestMapReduce2 extends Configured implements Tool {
	private final SimpleDateFormat df = new SimpleDateFormat(
			"yyyy-MM-dd-HH-mm-ss");

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJarByClass(TestMapReduce2.class);
		job.setJobName("test_map_reduce");

		FileInputFormat.setInputPaths(job,
				"/kafka/t_playbgn_v2/hourly/2014/07/30/19");
		FileOutputFormat.setOutputPath(job,
				new Path("/tmp/hexh/" + df.format(new Date())));

		// job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(LongWritable.class);

		// job.setOutputKeyClass(Text.class);
		// job.setOutputValueClass(LongWritable.class);

		// job.setMapperClass(TestMapper.class);
		// job.setReducerClass(TestReducer.class);

		job.setInputFormatClass(AvroKeyInputFormat.class);
		// job.setInputFormatClass(TextInputFormat.class);
		AvroJob.setInputKeySchema(job, OrderPlayBgnReqV2.getClassSchema());
		job.setNumReduceTasks(0);

		job.submit();
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "root");
		ToolRunner.run(new TestMapReduce2(), args);
	}

}
