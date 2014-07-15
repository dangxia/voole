package com.voole.hobbit.camus.etl;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import com.voole.hobbit.camus.coders.CamusWrapper;

/**
 *
 *
 */
public interface RecordWriterProvider {

	String getFilenameExtension();

	RecordWriter<IEtlKey, CamusWrapper<?>> getDataRecordWriter(
			TaskAttemptContext context, String fileName, CamusWrapper<?> data,
			FileOutputCommitter committer) throws IOException,
			InterruptedException;
}
