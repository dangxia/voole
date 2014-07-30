package com.voole.hobbit.mapreduce.test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroKeyRecordReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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
			Mapper<AvroKey<OrderPlayBgnReqV2>, NullWritable, Text, NullWritable> {
		Text t = new Text();

		@Override
		protected void map(AvroKey<OrderPlayBgnReqV2> key, NullWritable value,
				Context context) throws IOException, InterruptedException {
			t.set(getSessid(key.datum()).toString());
			context.write(t, NullWritable.get());
		}

		private CharSequence getSessid(SpecificRecordBase record) {
			if (record instanceof OrderPlayBgnReqV2) {
				return ((OrderPlayBgnReqV2) record).getSessID();
			} else if (record instanceof OrderPlayEndReqV2) {
				return ((OrderPlayEndReqV2) record).getSessID();
			}
			return "";
		}
	}

	public static class TestReducer2 extends
			Reducer<Text, NullWritable, Text, NullWritable> {
		private Text write = new Text();

		@Override
		protected void reduce(Text key, Iterable<NullWritable> iterable,
				Context context) throws IOException, InterruptedException {

			long count = 0l;
			for (NullWritable avroValue : iterable) {
				count++;
			}
			write.set(key.toString() + "_" + count);
			context.write(write, NullWritable.get());

		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		job.getConfiguration().setBoolean(
				MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
		job.setJarByClass(TestMapReduce2.class);
		job.setJobName("test_map_reduce");

		FileInputFormat.setInputPaths(job,
				"/kafka/t_playbgn_v2/hourly/2014/07/30/19");
		// FileInputFormat.addInputPath(job, new Path(
		// "/kafka/t_playend_v2/hourly/2014/07/30/19"));
		FileOutputFormat.setOutputPath(job,
				new Path("/tmp/hexh/" + df.format(new Date())));

		// List<Schema> schemas = new ArrayList<Schema>();
		// schemas.add(OrderPlayBgnReqV2.getClassSchema());
		// schemas.add(OrderPlayEndReqV2.getClassSchema());
		// Schema union = Schema.createUnion(schemas);
		//
		org.apache.avro.mapreduce.AvroJob.setInputKeySchema(job,
				OrderPlayBgnReqV2.getClassSchema());
		org.apache.avro.mapreduce.AvroJob.setOutputKeySchema(job,
				OrderPlayBgnReqV2.getClassSchema());
		// job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(NullWritable.class);
		// job.setOutputKeyClass(Text.class);
		// job.setOutputValueClass(NullWritable.class);
		//
		// job.setMapperClass(TestMapper2.class);
		// job.setReducerClass(TestReducer2.class);
		//

		// job.setInputFormatClass(TextInputFormat.class);
		job.setInputFormatClass(AvroKeyInputFormat.class);
		// job.setOutputFormatClass(AvroKeyOutputFormat.class);
		// job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(0);

		// job.submit();
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "root");
		ToolRunner.run(new TestMapReduce2(), args);
	}

	@InterfaceAudience.Public
	@InterfaceStability.Stable
	public static class AvroKeyInputFormat<T> extends
			FileInputFormat<AvroKey<T>, NullWritable> {
		private static final Logger LOG = LoggerFactory
				.getLogger(AvroKeyInputFormat.class);

		/** {@inheritDoc} */
		@Override
		public RecordReader<AvroKey<T>, NullWritable> createRecordReader(
				InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			Schema readerSchema = AvroJob.getInputKeySchema(context
					.getConfiguration());
			if (null == readerSchema) {
				LOG.warn("Reader schema was not set. Use AvroJob.setInputKeySchema() if desired.");
				LOG.info("Using a reader schema equal to the writer schema.");
			}
			LOG.info(AvroKeyRecordReader.class.getProtectionDomain().toString());
			return new AvroKeyRecordReader<T>(readerSchema);
		}
	}

}
