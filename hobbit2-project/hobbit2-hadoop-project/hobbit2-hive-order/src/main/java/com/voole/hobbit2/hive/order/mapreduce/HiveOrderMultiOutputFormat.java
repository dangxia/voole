package com.voole.hobbit2.hive.order.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HiveOrderMultiOutputFormat extends
		FileOutputFormat<Object, Object> {

	@Override
	public RecordWriter<Object, Object> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		return new HiveOrderMultiRecordWriter();
	}

	class HiveOrderMultiRecordWriter extends RecordWriter<Object, Object> {

		@Override
		public void write(Object key, Object value) throws IOException,
				InterruptedException {

		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {

		}

	}

}
