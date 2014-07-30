package com.voole.hobbit.mapreduce.test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voole.hobbit.avro.termial.OrderPlayBgnReqV2;
import com.voole.hobbit.avro.termial.OrderPlayEndReqV2;

public class TestMapReduce2 extends Configured implements Tool {
	private final SimpleDateFormat df = new SimpleDateFormat(
			"yyyy-MM-dd-HH-mm-ss");
	private static final Logger logger = LoggerFactory
			.getLogger(TestMapReduce2.class);

	public static class TestMapper2
			extends
			Mapper<AvroKey<SpecificRecordBase>, NullWritable, Text, AvroValue<Long>> {
		private Text sessId = new Text();
		private AvroValue<Long> v = new AvroValue<Long>();

		@Override
		protected void map(AvroKey<SpecificRecordBase> key, NullWritable value,
				Context context) throws IOException, InterruptedException {
			sessId.set((String) key.datum().get("sessID"));
			v.datum(1l);
			context.write(sessId, v);
		}
	}

	public static class TestReducer2 extends
			Reducer<Text, AvroValue<Long>, Text, NullWritable> {
		private Text write = new Text();

		@Override
		protected void reduce(Text key, Iterable<AvroValue<Long>> iterable,
				Context context) throws IOException, InterruptedException {

			// long bgn = 0l;
			// long end = 0l;
			// for (AvroValue<SpecificRecordBase> avroValue : iterable) {
			// logger.info("record type:"
			// + avroValue.datum().getClass().getName());
			// if (avroValue.datum() instanceof OrderPlayBgnReqV2) {
			// bgn++;
			// } else {
			// end++;
			// }
			// }
			// write.set(key.toString() + "_" + bgn + "_" + end);
			// context.write(write, NullWritable.get());

		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJarByClass(TestMapReduce2.class);
		job.setJobName("test_map_reduce");

		FileInputFormat.setInputPaths(job,
				"/kafka/t_playbgn_v2/hourly/2014/07/30/19");
		FileInputFormat.addInputPath(job, new Path(
				"/kafka/t_playend_v2/hourly/2014/07/30/19"));
		FileOutputFormat.setOutputPath(job,
				new Path("/tmp/hexh/" + df.format(new Date())));

		job.setMapOutputKeyClass(Text.class);

		AvroJob.setMapOutputValueSchema(job, Schema.create(Type.LONG));
		// AvroJob.setMapOutputValueSchema(
		// job,
		// SchemaBuilder.unionOf()
		// .type(OrderPlayBgnReqV2.getClassSchema()).and()
		// .type(OrderPlayEndReqV2.getClassSchema()).endUnion());

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapperClass(TestMapper2.class);
		job.setReducerClass(TestReducer2.class);

		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(0);

		AvroJob.setInputKeySchema(
				job,
				SchemaBuilder.unionOf()
						.type(OrderPlayBgnReqV2.getClassSchema()).and()
						.type(OrderPlayEndReqV2.getClassSchema()).endUnion());

		job.submit();
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "root");
		ToolRunner.run(new TestMapReduce2(), args);
	}

}
