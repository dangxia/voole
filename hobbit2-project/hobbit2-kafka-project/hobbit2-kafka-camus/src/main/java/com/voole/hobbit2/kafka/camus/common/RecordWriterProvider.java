package com.voole.hobbit2.kafka.camus.common;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

public interface RecordWriterProvider {

	String getFilenameExtension();

	RecordWriter<ICamusKey, Object> getDataRecordWriter(
			TaskAttemptContext context, FileOutputCommitter committer,
			ICamusKey key, Object record) throws IOException,
			InterruptedException;
}
