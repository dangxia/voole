/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.mr.mapreduce;

import java.io.IOException;

import kafka.message.Message;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.voole.hobbit2.camus.api.TopicMeta;
import com.voole.hobbit2.camus.api.transform.TransformException;
import com.voole.hobbit2.camus.mr.CamusMetaConfigs;
import com.voole.hobbit2.camus.mr.common.CamusKey;
import com.voole.hobbit2.camus.mr.common.KafkaReduceReader;
import com.voole.hobbit2.camus.mr.partitioner.ICamusPartitioner;
import com.voole.hobbit2.common.config.KafkaMetaConfigs;
import com.voole.hobbit2.tools.kafka.partition.TopicPartition;

/**
 * @author XuehuiHe
 * @date 2014年9月2日
 */
public class CamusRecordReader extends
		RecordReader<CamusKey, SpecificRecordBase> {
	private static final Logger log = LoggerFactory
			.getLogger(CamusRecordReader.class);

	private Mapper<CamusKey, Writable, CamusKey, Writable>.Context mapperContext;
	private TaskAttemptContext context;

	private CamusInputSplit split;
	private TopicPartition topicPartition;

	private long total;
	private long readedNum = 0;
	private KafkaReduceReader reader;

	private final BytesWritable msgValue = new BytesWritable();
	private final BytesWritable msgKey = new BytesWritable();

	private CamusKey key;

	private SpecificRecordBase value;
	private final Text errorRecord = new Text();

	private TopicMeta topicMeta;

	private ICamusPartitioner partitioner;

	public CamusRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		initialize(split, context);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		this.split = (CamusInputSplit) split;
		this.key = new CamusKey(this.split);
		this.total = this.split.getLength();
		this.topicPartition = this.split.getBrokerAndTopicPartition()
				.getPartition();
		log.info(this.topicPartition + " start from :" + this.split.getOffset()
				+ ", end with:" + this.split.getLatestOffset()
				+ ",RecordReader initialize");
		this.context = context;
		if (context instanceof Mapper.Context) {
			mapperContext = (Mapper.Context) context;
		}

		try {
			topicMeta = CamusMetaConfigs.getTopicMetaManager(context)
					.findTopicMeta(topicPartition.getTopic());
			partitioner = (ICamusPartitioner) CamusMetaConfigs
					.getPartitionerManager(context).findPartitioner(
							topicPartition.getTopic());
		} catch (TransformException e) {
			Throwables.propagate(e);
		}

	}

	private static byte[] getBytes(BytesWritable val) {
		byte[] buffer = val.getBytes();
		long len = val.getLength();
		byte[] bytes = buffer;
		if (len < buffer.length) {
			bytes = new byte[(int) len];
			System.arraycopy(buffer, 0, bytes, 0, (int) len);
		}

		return bytes;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		Message message = null;
		try {
			if (reader == null) {
				reader = new KafkaReduceReader(context, split, context
						.getConfiguration().getInt(
								KafkaMetaConfigs.KAFKA_TIME_OUT_MS, 40000),
						context.getConfiguration().getInt(
								KafkaMetaConfigs.KAFKA_BUFFER_SIZE_BYTES, 10240));
			}
			while (reader.getNext(key, msgValue, msgKey)) {
				if (key.getOffset() > split.getLatestOffset()) {
					return false;
				}
				context.progress();
				byte[] valueBytes = getBytes(msgValue);
				byte[] keyBytes = getBytes(msgKey);
				if (keyBytes.length == 0) {
					message = new Message(valueBytes);
				} else {
					message = new Message(valueBytes, keyBytes);
				}
				long checksum = key.getChecksum();
				if (checksum != message.checksum()) {
					throw new ChecksumException("Invalid message checksum "
							+ message.checksum() + ". Expected "
							+ key.getChecksum(), key.getOffset());
				}
				context.getCounter("fetch_topic_num", key.getTopic())
						.increment(1l);
				try {
					Optional<SpecificRecordBase> ov = this.topicMeta
							.getTransformer().transform(valueBytes);
					if (!ov.isPresent()) {
						continue;
					}
					this.value = ov.get();
					Long stamp = this.topicMeta.getStampFinder().findStamp(
							this.value);
					if (stamp == null) {
						context.getCounter("transform_failed", key.getTopic())
								.increment(1l);
						errorRecord.set(valueBytes);
						mapperContext.write(key, errorRecord);
						continue;
					}
					key.setStamp(partitioner.getPartitionStamp(stamp));
				} catch (TransformException e) {
					context.getCounter("transform_failed", key.getTopic())
							.increment(1l);
					errorRecord.set(valueBytes);
					mapperContext.write(key, errorRecord);
					continue;
				}
				return true;
			}
			reader = null;
		} catch (Throwable t) {
			Throwables.propagate(t);
		}
		return false;
	}

	@Override
	public CamusKey getCurrentKey() throws IOException {
		return key;
	}

	@Override
	public SpecificRecordBase getCurrentValue() throws IOException,
			InterruptedException {
		return value;
	}

	private long getPos() throws IOException {
		if (reader != null) {
			return readedNum + reader.getReadedNum();
		} else {
			return readedNum;
		}
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (getPos() == 0) {
			return 0f;
		}

		if (getPos() >= total) {
			return 1f;
		}
		return (float) ((double) getPos() / total);
	}

	@Override
	public void close() throws IOException {
		closeReader();
	}

	private void closeReader() throws IOException {
		if (reader != null) {
			try {
				readedNum += reader.getReadedNum();
				reader.close();
			} catch (Exception e) {
				// not much to do here but skip the task
			} finally {
				reader = null;
			}
		}
	}

}
