package com.voole.hobbit.mapreduce.test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voole.hobbit.avro.termial.OrderPlayBgnReqV2;
import com.voole.hobbit.avro.termial.OrderPlayEndReqV2;

public class TestMapReduce3 extends Configured implements Tool {
	private final SimpleDateFormat df = new SimpleDateFormat(
			"yyyy-MM-dd-HH-mm-ss");
	private static final Logger logger = LoggerFactory
			.getLogger(TestMapReduce3.class);

	public static class TestMapper3
			extends
			AvroMapper<SpecificRecordBase, Pair<CharSequence, SpecificRecordBase>> {
		@Override
		public void map(
				SpecificRecordBase datum,
				AvroCollector<Pair<CharSequence, SpecificRecordBase>> collector,
				Reporter reporter) throws IOException {
			collector.collect(new Pair<CharSequence, SpecificRecordBase>(datum
					.get("sessID").toString(), datum));
		}
	}

	public static class TestReducer3
			extends
			AvroReducer<CharSequence, SpecificRecordBase, Pair<CharSequence, Integer>> {
		@Override
		public void reduce(CharSequence key,
				Iterable<SpecificRecordBase> values,
				AvroCollector<Pair<CharSequence, Integer>> collector,
				Reporter reporter) throws IOException {
			int count = 0;
			for (SpecificRecordBase SpecificRecordBase : values) {
				count++;
			}
			collector.collect(new Pair<CharSequence, Integer>(key, count));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), TestMapReduce3.class);
		conf.setJobName("test_avro_map_reduce");

		FileInputFormat.setInputPaths(conf,
				"/kafka/t_playbgn_v2/hourly/2014/07/30/19");
		FileInputFormat.addInputPath(conf, new Path(
				"/kafka/t_playend_v2/hourly/2014/07/30/19"));
		FileOutputFormat.setOutputPath(conf,
				new Path("/tmp/hexh/" + df.format(new Date())));

		AvroJob.setMapperClass(conf, TestMapper3.class);
		AvroJob.setReducerClass(conf, TestReducer3.class);

		// Note that AvroJob.setInputSchema and AvroJob.setOutputSchema set
		// relevant config options such as input/output format, map output
		// classes, and output key class.
		Schema s = SchemaBuilder.unionOf()
				.type(OrderPlayBgnReqV2.getClassSchema()).and()
				.type(OrderPlayEndReqV2.getClassSchema()).endUnion();
		AvroJob.setInputSchema(conf, s);
		AvroJob.setMapOutputSchema(conf,
				Pair.getPairSchema(Schema.create(Type.STRING), s));
		AvroJob.setOutputSchema(
				conf,
				Pair.getPairSchema(Schema.create(Type.STRING),
						Schema.create(Type.INT)));
		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "root");
		int res = ToolRunner.run(new Configuration(), new TestMapReduce3(),
				args);
		System.exit(res);
	}

}
