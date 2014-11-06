/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.voole.hobbit2.storm.order.partition.StormOrderSpoutPartitionFetcher;
import com.voole.hobbit2.tools.kafka.KafkaUtils;
import com.voole.hobbit2.tools.kafka.partition.BrokerAndTopicPartition;
import com.voole.hobbit2.tools.kafka.partition.TopicPartition;

/**
 * @author XuehuiHe
 * @date 2014年9月22日
 */
public class StormOrderHDFSUtils {
	private static final Logger log = LoggerFactory
			.getLogger(StormOrderSpoutPartitionFetcher.class);
	public static final Configuration conf = new Configuration();
	static {
		conf.addResource("core-site2.xml");
		conf.setBoolean("order.detail.transformer.is.auto.refresh.cache", true);
	}

	public static void main(String[] args) throws IOException {
		FileSystem.get(conf);
	}

	public static FileReader<SpecificRecordBase> getNoendReader(Path path)
			throws IOException {
		SeekableInput input = new FsInput(path, conf);
		DatumReader<SpecificRecordBase> reader = new SpecificDatumReader<SpecificRecordBase>();
		FileReader<SpecificRecordBase> fileReader = DataFileReader.openReader(
				input, reader);
		return fileReader;
	}

	private static Map<TopicPartition, Long> readPrevPartionsStates()
			throws IOException {
		return readPrevPartionsStates(
				getPreviousExecPath(StormOrderMetaConfigs
						.getHiveOrderExecHistoryPath()),
				StormOrderMetaConfigs.CAMUS_REQUESTS_FILE);
	}

	public static Optional<Long> findOffset(
			BrokerAndTopicPartition brokerAndTopicPartition) throws IOException {
		Optional<Long> hiveOffset = getHiveOrderOffset(brokerAndTopicPartition
				.getPartition());
		Optional<Long> kakfaEarliestOffset = KafkaUtils
				.findEarliestOffset(brokerAndTopicPartition);
		if (!kakfaEarliestOffset.isPresent()) {
			return Optional.absent();
		}
		long kakfaOffset = kakfaEarliestOffset.get() - 1;
		if (!hiveOffset.isPresent()) {
			return Optional.of(kakfaOffset);
		}
		if (kakfaOffset > hiveOffset.get()) {
			log.warn(brokerAndTopicPartition + " kafka offset:" + kakfaOffset
					+ ",hive order offset:" + hiveOffset.get() + ",reset");
			return Optional.of(kakfaOffset);
		}
		return hiveOffset;
	}

	public static Optional<Long> getHiveOrderOffset(TopicPartition partition)
			throws IOException {
		Map<TopicPartition, Long> partitionsMap = readPrevPartionsStates();
		if (partitionsMap.containsKey(partition)) {
			return Optional.of(partitionsMap.get(partition));
		}
		return Optional.absent();
	}

	private static Map<TopicPartition, Long> readPrevPartionsStates(
			Optional<Path> previousPath, String name) throws IOException {
		Map<TopicPartition, Long> result = new HashMap<TopicPartition, Long>();
		if (!previousPath.isPresent()) {
			return result;
		}
		Path path = new Path(previousPath.get(), name);
		if (!FileSystem.get(conf).exists(path)) {
			log.warn("file:" + name + "don't exists");
			return result;
		}
		log.info("readPrevPartionsStates file:" + path.toUri().getPath());
		SequenceFile.Reader reader = new SequenceFile.Reader(conf,
				SequenceFile.Reader.file(path));
		TopicPartition key = new TopicPartition();
		LongWritable offset = new LongWritable();
		while (reader.next(key, offset)) {
			long oldOffset = 0;
			if (result.containsKey(key)) {
				oldOffset = result.get(key);
			}
			if (oldOffset < offset.get()) {
				result.put(key, offset.get());
			}
			key = new TopicPartition();
		}
		reader.close();
		return result;

	}

	private static Optional<Path> getPreviousExecPath(Path execHistoryPath)
			throws FileNotFoundException, IOException {
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] executions = fs.listStatus(execHistoryPath);
		if (executions.length > 0) {
			Path previous = executions[executions.length - 1].getPath();
			log.info("Previous execution: " + previous.toString());
			return Optional.of(previous);
		} else {
			log.info("No previous execution, all topics pulled from earliest available offset");
			return Optional.absent();
		}
	}

	protected static List<Path> getNoendFilePaths(String topic, int partition,
			int partitions) throws IOException {
		List<Path> paths = getNoendFilePaths(topic);
		List<Path> result = new ArrayList<Path>();

		int size = paths.size();
		int index = partition;
		while (index < size) {
			result.add(paths.get(index));
			index += partitions;
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	public static List<Path> getNoendFilePaths(final String topic)
			throws IOException {
		Optional<Path> prevExecPath = getPreviousExecPath(StormOrderMetaConfigs
				.getHiveOrderExecHistoryPath());
		if (!prevExecPath.isPresent()) {
			return Collections.EMPTY_LIST;
		}
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fileStatus = fs.listStatus(prevExecPath.get(),
				new PathFilter() {

					@Override
					public boolean accept(Path path) {
						return path.getName().startsWith(
								StormOrderMetaConfigs.HIVE_ORDER_NOEND_PREFIX)
								&& (path.getName().indexOf(topic) != -1);
					}
				});
		List<Path> paths = new ArrayList<Path>();
		for (FileStatus fileStatus2 : fileStatus) {
			paths.add(fileStatus2.getPath());
		}

		return paths;
	}

}
