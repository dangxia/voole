package com.voole.hobbit.camus.etl.kafka.common;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.voole.hobbit.camus.coders.CamusWrapper;
import com.voole.hobbit.camus.etl.IEtlKey;
import com.voole.hobbit.camus.etl.RecordWriterProvider;

/**
 * Provides a RecordWriter that uses FSDataOutputStream to write a String
 * record as bytes to HDFS without any reformatting or compession.
 */
public class StringRecordWriterProvider implements RecordWriterProvider {
	public static final String ETL_OUTPUT_RECORD_DELIMITER = "etl.output.record.delimiter";
	public static final String DEFAULT_RECORD_DELIMITER = "";

	protected String recordDelimiter = null;

	// TODO: Make this configurable somehow.
	// To do this, we'd have to make RecordWriterProvider have an
	// init(JobContext context) method signature that EtlMultiOutputFormat would
	// always call.
	@Override
	public String getFilenameExtension() {
		return "";
	}

	@Override
	public RecordWriter<IEtlKey, CamusWrapper<?>> getDataRecordWriter(
			TaskAttemptContext context, String fileName,
			CamusWrapper<?> camusWrapper, FileOutputCommitter committer)
			throws IOException, InterruptedException {

		// If recordDelimiter hasn't been initialized, do so now
		if (recordDelimiter == null) {
			recordDelimiter = context.getConfiguration().get(
					ETL_OUTPUT_RECORD_DELIMITER, DEFAULT_RECORD_DELIMITER);
		}

		// Get the filename for this RecordWriter.
		Path path = new Path(committer.getWorkPath(),
				FileOutputFormat.getUniqueFile(context, fileName,
						getFilenameExtension()));

		// Create a FSDataOutputStream stream that will write to path.
		final FSDataOutputStream writer = path.getFileSystem(
				context.getConfiguration()).create(path);

		// Return a new anonymous RecordWriter that uses the
		// FSDataOutputStream writer to write bytes straight into path.
		return new RecordWriter<IEtlKey, CamusWrapper<?>>() {
			@Override
			public void write(IEtlKey ignore, CamusWrapper<?> data)
					throws IOException {
				String record = (String) data.getRecord() + recordDelimiter;
				writer.write(record.getBytes());
			}

			@Override
			public void close(TaskAttemptContext context) throws IOException,
					InterruptedException {
				writer.close();
			}
		};
	}
}
