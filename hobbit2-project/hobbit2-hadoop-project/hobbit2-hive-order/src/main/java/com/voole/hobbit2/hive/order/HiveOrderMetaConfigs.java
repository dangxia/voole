package com.voole.hobbit2.hive.order;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.voole.hobbit2.camus.api.TopicMeta;
import com.voole.hobbit2.camus.api.TopicMetaManager;
import com.voole.hobbit2.camus.api.TopicMetaRegister;
import com.voole.hobbit2.camus.api.transform.TransformException;

public class HiveOrderMetaConfigs {
	private static final Logger log = LoggerFactory
			.getLogger(HiveOrderMetaConfigs.class);

	public static final String CAMUS_REQUESTS_FILE = "previous.partition.states";
	public static final String CAMUS_OFFSET_PREFIX = "offsets";

	public static final String NOEND_PREFIX = "noend_";

	public static final String FILE_INFO_PREFIX = "hive_table_file_info_";

	public static final String CAMUS_DEST_PATH = "camus.dest.path";
	public static final String CAMUS_EXEC_HISTORY_PATH = "camus.exec.history.path";
	public static final String CURR_CAMUS_EXEC_TIME = "curr.camus.exec.time";
	public static final String PREV_CAMUS_EXEC_TIME = "prev.camus.exec.time";
	public static final String PREV_CAMUS_EXEC_TIME_FILE_NAME = "prev_camus_exec_time.save";

	public static final String JOB_NAME = "hive.order.job.name";
	public static final String JOB_MAPS = "hive.order.job.maps";
	public static final String JOB_REDUCES = "hive.order.job.reduces";

	public static final String WHITELIST_TOPICS = "hive.order.whitelist.topics";

	public static final String EXEC_BASE_PATH = "hive.order.exec.base.path";
	public static final String EXEC_HISTORY_PATH = "hive.order.exec.history.path";
	public static final String EXEC_HISTORY_MAX_OF_QUOTA = "hive.order.exec.history.max.of.quota";

	public static final String EXEC_START_TIME = "hive.order.exec.start.time";

	public static final String AD_NIELSEN_IS_SEND = "ad.nielsen.is.send";
	public static final String AD_TRANSFORMER_IS_TO_RUN = "ad.transformer.is.to.run";

	public static final String TOPIC_MEATA_REGISTERS = "hive.order.topic.meta.registers";

	private volatile static TopicMetaManager topicManager;

	public static TopicMetaManager getTopicMetaManager(JobContext job) {
		if (topicManager == null) {
			try {
				createTopicMetaManager(job);
			} catch (Exception e) {
				log.error(HiveOrderMetaConfigs.class
						+ " getTopicMetaManager error", e);
				Throwables.propagate(e);
			}
		}
		return topicManager;
	}

	private synchronized static void createTopicMetaManager(JobContext job)
			throws TransformException {
		if (topicManager != null) {
			return;
		}
		TopicMetaManager _topicManager = new TopicMetaManager();
		List<TopicMetaRegister> registers = job.getConfiguration()
				.getInstances(TOPIC_MEATA_REGISTERS, TopicMetaRegister.class);
		_topicManager.register(registers.toArray(new TopicMetaRegister[] {}));
		topicManager = _topicManager;

	}

	public static Schema getOrderUnionSchema(JobContext job) {
		String[] topics = HiveOrderMetaConfigs.getWhiteTopics(job);
		List<Schema> schemas = new ArrayList<Schema>();
		TopicMetaManager topicMetaManager = getTopicMetaManager(job);
		for (String topic : topics) {
			TopicMeta topicMeta = topicMetaManager.findTopicMeta(topic);
			if (topicMeta != null) {
				schemas.add(topicMeta.getSchema());
			}
		}
		return Schema.createUnion(schemas);
	}

	public static Map<Class<?>, String> getClassToTopic(JobContext job) {
		TopicMetaManager topicMetaManager = getTopicMetaManager(job);
		Map<String, TopicMeta> topicToTopicMeta = topicMetaManager
				.getTopicToMeta();
		Map<Class<?>, String> classToTopic = new HashMap<Class<?>, String>();
		for (Entry<String, TopicMeta> entry : topicToTopicMeta.entrySet()) {
			TopicMeta topicMeta = entry.getValue();
			classToTopic.put(topicMeta.getClazz(), topicMeta.getTopic());
		}
		return classToTopic;
	}

	public static Map<String, Schema> getTopicToSchema(JobContext job) {
		TopicMetaManager topicMetaManager = getTopicMetaManager(job);
		Map<String, TopicMeta> topicToTopicMeta = topicMetaManager
				.getTopicToMeta();
		Map<String, Schema> topicToSchema = new HashMap<String, Schema>();
		for (Entry<String, TopicMeta> entry : topicToTopicMeta.entrySet()) {
			TopicMeta topicMeta = entry.getValue();
			topicToSchema.put(topicMeta.getTopic(), topicMeta.getSchema());
		}
		return topicToSchema;
	}

	public static Path getCamusDestPath(JobContext job) {
		return new Path(job.getConfiguration().get(CAMUS_DEST_PATH));
	}

	public static Path getCamusExecHistoryPath(JobContext job) {
		return new Path(job.getConfiguration().get(CAMUS_EXEC_HISTORY_PATH));
	}

	public static void setCurrCamusExecTime(JobContext job,
			long currCamusExecTime) {
		job.getConfiguration().setLong(CURR_CAMUS_EXEC_TIME, currCamusExecTime);
	}

	public static long getCurrCamusExecTime(JobContext job) {
		return job.getConfiguration().getLong(CURR_CAMUS_EXEC_TIME,
				System.currentTimeMillis());
	}

	public static void setPrevCamusExecTime(JobContext job,
			long prevCamusExecTime) {
		job.getConfiguration().setLong(PREV_CAMUS_EXEC_TIME, prevCamusExecTime);
	}

	public static long getPrevCamusExecTime(JobContext job) {
		return job.getConfiguration().getLong(PREV_CAMUS_EXEC_TIME,
				System.currentTimeMillis());
	}

	public static String[] getWhiteTopics(JobContext job) {
		return job.getConfiguration().getStrings(WHITELIST_TOPICS);
	}

	public static void setExecStartTime(JobContext job) {
		job.getConfiguration().setLong(EXEC_START_TIME,
				System.currentTimeMillis());
	}

	public static long getExecStartTime(JobContext job) {
		return job.getConfiguration().getLong(EXEC_START_TIME,
				System.currentTimeMillis());
	}

	public static String getJobName(JobContext job) {
		return job.getConfiguration().get(JOB_NAME);
	}

	public static int getJobMaps(JobContext job) {
		return job.getConfiguration().getInt(JOB_MAPS, 20);
	}

	public static int getJobReduces(JobContext job) {
		return job.getConfiguration().getInt(JOB_REDUCES, 20);
	}

	public static Path getExecBasePath(JobContext job) {
		return new Path(job.getConfiguration().get(EXEC_BASE_PATH));
	}

	public static Path getExecHistoryPath(JobContext job) {
		return new Path(job.getConfiguration().get(EXEC_HISTORY_PATH));
	}

	public static float getExecHistoryMaxOfQuota(JobContext job) {
		return job.getConfiguration().getFloat(EXEC_HISTORY_MAX_OF_QUOTA, .5f);
	}

	public static boolean isRunadPlayLogTransformer(JobContext job) {
		return job.getConfiguration().getBoolean(AD_TRANSFORMER_IS_TO_RUN,
				false);
	}
}
