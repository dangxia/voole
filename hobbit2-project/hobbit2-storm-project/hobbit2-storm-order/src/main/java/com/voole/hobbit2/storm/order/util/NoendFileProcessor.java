package com.voole.hobbit2.storm.order.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.file.FileReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;

import com.voole.hobbit2.storm.order.StormOrderHDFSUtils;
import com.voole.hobbit2.storm.order.state.ExtraInfoQueryState;
import com.voole.hobbit2.storm.order.state.ExtraInfoQueryStateImpl;
import com.voole.hobbit2.storm.order.state.SessionState;
import com.voole.hobbit2.storm.order.state.SessionStateImpl;

public class NoendFileProcessor {
	private final static Logger log = LoggerFactory
			.getLogger(NoendFileProcessor.class);

	private static final ExtraInfoQueryState extraInfoQueryState = new ExtraInfoQueryStateImpl();
	private static final SessionState sessionState = new SessionStateImpl();
	private static final List<SpecificRecordBase> drys = new ArrayList<SpecificRecordBase>();

	public static void processNoendFiles(Config conf) throws IOException {
		Map<String, List<Path>> topicToNoendPaths = StormOrderHDFSUtils
				.getWhiteTopicToNoendPaths(conf);
		for (Entry<String, List<Path>> entry : topicToNoendPaths.entrySet()) {
			for (Path path : entry.getValue()) {
				FileReader<SpecificRecordBase> reader = StormOrderHDFSUtils
						.getNoendReader(path);
				log.info("start : read noend records from file:"
						+ path.toUri().getPath());
				SpecificRecordBase recordBase = null;
				long count = 0l;
				while (reader.hasNext()) {
					recordBase = reader.next();
					emit(recordBase);
					count++;
				}
				reader.close();
				log.info("end : count: " + count
						+ " ,read noend records from file:"
						+ path.toUri().getPath());
			}
		}
		flush();

	}

	protected static void emit(SpecificRecordBase recordBase) {
		if (drys.size() >= 5000) {
			flush();
		}
		try {
			KafkaRecordDehydration.dry(recordBase);
			if (recordBase != null) {
				drys.add(recordBase);
			}
		} catch (Exception e) {
			log.warn("record dry failed", e);
		}
	}

	public static void flush() {
		sessionState.update(extraInfoQueryState.queryRecords(drys));
		drys.clear();
	}
}
