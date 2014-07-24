package com.voole.hobbit.camus.etl.kafka.common;

import java.io.IOException;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.DataFileWriter.AppendWriteException;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.voole.hobbit.camus.coders.CamusWrapper;
import com.voole.hobbit.camus.etl.IEtlKey;
import com.voole.hobbit.camus.etl.RecordWriterProvider;
import com.voole.hobbit.camus.etl.kafka.CamusConfigs;

/**
 *
 *
 */
public class AvroRecordWriterProvider implements RecordWriterProvider {
	public final static String EXT = ".avro";
	private static Logger log = Logger
			.getLogger(AvroRecordWriterProvider.class);

	@Override
	public String getFilenameExtension() {
		return EXT;
	}

	@Override
	public RecordWriter<IEtlKey, CamusWrapper<?>> getDataRecordWriter(
			TaskAttemptContext context, String fileName, CamusWrapper<?> data,
			FileOutputCommitter committer) throws IOException,
			InterruptedException {
		final DataFileWriter<Object> writer = new DataFileWriter<Object>(
				new SpecificDatumWriter<Object>());

		if (FileOutputFormat.getCompressOutput(context)) {
			if ("snappy".equals(CamusConfigs.getEtlOutputCodec(context))) {
				writer.setCodec(CodecFactory.snappyCodec());
			} else {
				int level = CamusConfigs.getEtlDeflateLevel(context);
				writer.setCodec(CodecFactory.deflateCodec(level));
			}
		}

		Path path = committer.getWorkPath();
		path = new Path(path, FileOutputFormat.getUniqueFile(context, fileName,
				EXT));
		writer.create(((GenericRecord) data.getRecord()).getSchema(), path
				.getFileSystem(context.getConfiguration()).create(path));

		writer.setSyncInterval(CamusConfigs
				.getEtlAvroWriterSyncInterval(context));

		return new RecordWriter<IEtlKey, CamusWrapper<?>>() {
			@Override
			public void write(IEtlKey ignore, CamusWrapper<?> data)
					throws IOException {
				try {
					writer.append(data.getRecord());
				} catch (AppendWriteException e) {
					Record r = (Record) data.getRecord();
					log.error("record name :" + r.getSchema().getName()
							+ "\t,record data:" + r.toString(), e);
				}

			}

			@Override
			public void close(TaskAttemptContext arg0) throws IOException,
					InterruptedException {
				writer.close();
			}
		};
	}
}
