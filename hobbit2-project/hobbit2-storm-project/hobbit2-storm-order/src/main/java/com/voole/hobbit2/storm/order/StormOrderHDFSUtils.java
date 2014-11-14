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
import java.util.Map.Entry;

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

import backtype.storm.Config;

import com.google.common.base.Optional;
import com.voole.hobbit2.tools.kafka.partition.Broker;
import com.voole.hobbit2.tools.kafka.partition.BrokerAndTopicPartition;
import com.voole.hobbit2.tools.kafka.partition.PartitionState;
import com.voole.hobbit2.tools.kafka.partition.TopicPartition;

/**
 * @author XuehuiHe
 * @date 2014年9月22日
 */
public class StormOrderHDFSUtils {
	private static final Logger log = LoggerFactory
			.getLogger(StormOrderHDFSUtils.class);
	public static final Configuration conf = new Configuration();

	public static volatile Map<TopicPartition, Long> topicPartitionToLastOffset;

	static {
		conf.addResource("core-site2.xml");
		conf.setBoolean("order.detail.transformer.is.auto.refresh.cache", true);
	}

	public static FileReader<SpecificRecordBase> getNoendReader(Path path)
			throws IOException {
		SeekableInput input = new FsInput(path, conf);
		DatumReader<SpecificRecordBase> reader = new SpecificDatumReader<SpecificRecordBase>();
		FileReader<SpecificRecordBase> fileReader = DataFileReader.openReader(
				input, reader);
		return fileReader;
	}

	public static Optional<Long> findOffset(
			BrokerAndTopicPartition brokerAndTopicPartition,
			@SuppressWarnings("rawtypes") Map stormConf) throws IOException {
		if (topicPartitionToLastOffset == null) {
			initTopicPartitionToLastOffset(stormConf);
		}
		TopicPartition topicPartition = brokerAndTopicPartition.getPartition();
		if (topicPartitionToLastOffset.containsKey(topicPartition)) {
			return Optional.of(topicPartitionToLastOffset.get(topicPartition));
		}
		return Optional.absent();
	}

	public static synchronized void initTopicPartitionToLastOffset(
			@SuppressWarnings("rawtypes") Map stormConf) throws IOException {
		if (topicPartitionToLastOffset != null) {
			return;
		}
		log.info("initTopicPartitionToLastOffset started");
		Map<TopicPartition, Long> result = new HashMap<TopicPartition, Long>();
		Map<TopicPartition, PartitionState> kafkaPartitionState = readKafkaPartitionState();
		Map<TopicPartition, Long> hivePrevPartionsStates = readHivePrevPartionsStates(stormConf);

		for (Entry<TopicPartition, PartitionState> entry : kafkaPartitionState
				.entrySet()) {
			TopicPartition topicPartition = entry.getKey();
			long kafkaEarliestOffset = entry.getValue().getEarliestOffset() - 1;
			long kafkaLatestOffset = entry.getValue().getLatestOffset() - 1;
			long hiveLatestOffset = -1;
			if (hivePrevPartionsStates.containsKey(topicPartition)) {
				hiveLatestOffset = hivePrevPartionsStates.get(topicPartition);
			}
			if (hiveLatestOffset < kafkaEarliestOffset) {
				log.warn(topicPartition
						+ " ,hiveLatestOffset < kafkaEarliestOffset ,hiveLatestOffset:"
						+ hiveLatestOffset + ",kafkaEarliestOffset:"
						+ kafkaEarliestOffset
						+ "set last offset to kafkaEarliestOffset");
				result.put(topicPartition, kafkaEarliestOffset);
			} else if (hiveLatestOffset > kafkaLatestOffset) {
				log.warn(topicPartition
						+ " ,hiveLatestOffset > kafkaLatestOffset ,hiveLatestOffset:"
						+ hiveLatestOffset + ",kafkaLatestOffset:"
						+ kafkaLatestOffset
						+ "set last offset to kafkaEarliestOffset:"
						+ kafkaEarliestOffset);
				result.put(topicPartition, kafkaLatestOffset);
			} else {
				log.info(topicPartition
						+ " ,set lastestoffset to hiveLatestOffset:"
						+ hiveLatestOffset);
				result.put(topicPartition, hiveLatestOffset);
			}
		}
		topicPartitionToLastOffset = result;
		log.info("initTopicPartitionToLastOffset ended");

	}

	public static Map<TopicPartition, PartitionState> readKafkaPartitionState() {
		Map<TopicPartition, PartitionState> result = new HashMap<TopicPartition, PartitionState>();
		Map<Broker, List<PartitionState>> data = StormOrderMetaConfigs
				.fetchPartitionState();
		for (Entry<Broker, List<PartitionState>> entry : data.entrySet()) {
			for (PartitionState state : entry.getValue()) {
				result.put(state.getBrokerAndTopicPartition().getPartition(),
						state);
			}
		}
		return result;
	}

	public static Map<TopicPartition, Long> readHivePrevPartionsStates(
			@SuppressWarnings("rawtypes") Map stormConf) throws IOException {
		Optional<Path> lastExecPath = StormOrderMetaConfigs
				.getHiveOrderLastExecPath(stormConf);
		String fileName = StormOrderMetaConfigs.CAMUS_REQUESTS_FILE;
		Map<TopicPartition, Long> result = new HashMap<TopicPartition, Long>();
		if (!lastExecPath.isPresent()) {
			log.info("hive order last exec path is empty ,empty HivePrevPartionsStates");
			return result;
		}
		Path path = new Path(lastExecPath.get(), fileName);
		if (!FileSystem.get(conf).exists(path)) {
			log.warn("hive order PrevPartionsStates file:" + fileName
					+ "not found,empty HivePrevPartionsStates");
			return result;
		}
		log.info("read hive order PrevPartionsStates file:"
				+ path.toUri().getPath());
		SequenceFile.Reader reader = new SequenceFile.Reader(conf,
				SequenceFile.Reader.file(path));
		TopicPartition key = new TopicPartition();
		LongWritable offset = new LongWritable();
		while (reader.next(key, offset)) {
			long oldOffset = -1;
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

	public static Optional<Path> findHiveOrderLastExecPath()
			throws FileNotFoundException, IOException {
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] executions = fs.listStatus(StormOrderMetaConfigs
				.getHiveOrderExecHistoryPath());
		if (executions.length > 0) {
			Path previous = executions[executions.length - 1].getPath();
			log.info("hive order last exec path " + previous.toUri().getPath());
			return Optional.of(previous);
		} else {
			log.info("No hive order last exec path");
			return Optional.absent();
		}
	}

	public static Map<String, List<Path>> getWhiteTopicToNoendPaths(
			@SuppressWarnings("rawtypes") Map stormConf) throws IOException {
		List<Path> noendPaths = getNoendFilePaths(stormConf);
		List<String> whiteTopics = StormOrderMetaConfigs.getWhiteTopics();
		Map<String, List<Path>> whiteTopicToNoendPaths = new HashMap<String, List<Path>>();
		for (Path noendPath : noendPaths) {
			String pathName = noendPath.getName();
			boolean useless = true;
			for (String topic : whiteTopics) {
				if (pathName.indexOf(topic) != -1) {
					useless = false;
					List<Path> paths = null;
					if (whiteTopicToNoendPaths.containsKey(topic)) {
						paths = whiteTopicToNoendPaths.get(topic);
					} else {
						paths = new ArrayList<Path>();
						whiteTopicToNoendPaths.put(topic, paths);
					}
					paths.add(noendPath);
					log.info("found topic:" + topic + " noend file:"
							+ noendPath.toUri().getPath());
					break;
				}
			}
			if (useless) {
				log.info("found useless noend file:"
						+ noendPath.toUri().getPath());
			}
		}
		return whiteTopicToNoendPaths;

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static List<Path> getNoendFilePaths(Map stormConf)
			throws IOException {
		Optional<Path> lastExecPath = StormOrderMetaConfigs
				.getHiveOrderLastExecPath(stormConf);
		if (!lastExecPath.isPresent()) {
			return Collections.EMPTY_LIST;
		}
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fileStatus = fs.listStatus(lastExecPath.get(),
				new PathFilter() {

					@Override
					public boolean accept(Path path) {
						return path.getName().startsWith(
								StormOrderMetaConfigs.HIVE_ORDER_NOEND_PREFIX);
					}
				});
		List<Path> paths = new ArrayList<Path>();
		for (FileStatus fileStatus2 : fileStatus) {
			paths.add(fileStatus2.getPath());
		}

		return paths;
	}

	public static void main(String[] args) throws FileNotFoundException,
			IOException {
		Optional<Path> hiveOrderLastExecPath = findHiveOrderLastExecPath();
		Config stormConf = new Config();
		if (hiveOrderLastExecPath.isPresent()) {
//			StormOrderMetaConfigs.setHiveOrderLastExecPath(stormConf,
//					hiveOrderLastExecPath.get().toUri().getPath());
		}
		getWhiteTopicToNoendPaths(stormConf);
		initTopicPartitionToLastOffset(stormConf);

	}

}
