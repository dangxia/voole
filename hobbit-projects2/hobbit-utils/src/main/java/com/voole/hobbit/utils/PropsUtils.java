/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.utils;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author XuehuiHe
 * @date 2014年5月28日
 */
public class PropsUtils {
	private static Logger logger = LoggerFactory.getLogger(PropsUtils.class);

	private static class MysqlPropertiesHolder {
		public static Properties mysqlProps = getProperties("/mysql/mysql.properties");
	}

	private static class KafkaPropertiesHolder {
		public static KafkaProperties kafkaProps = getKafkaProperties2();
	}

	public static KafkaProperties getKafkaProperties() {
		return KafkaPropertiesHolder.kafkaProps;
	}

	public static Properties getMysqlProperties() {
		return MysqlPropertiesHolder.mysqlProps;
	}

	private static KafkaProperties getKafkaProperties2() {
		return (KafkaProperties) getProperties(new KafkaProperties(),
				"/kafka/kafka.properties");
	}

	public static Properties getProperties(Properties properties,
			String... paths) {
		for (String path : paths) {
			try {
				properties.load(PropsUtils.class.getResourceAsStream(path));
			} catch (Exception e) {
				logger.warn("load file:" + path + " error", e);
			}
		}
		return properties;
	}

	public static Properties getProperties(String... paths) {
		Properties properties = new Properties();
		getProperties(properties, paths);
		return properties;
	}

	public static class KafkaProperties extends Properties {
		public String getConnect() {
			return getProperty("zookeeper.connect");
		}

		public int getSessionTimeoutMs() {
			return Integer
					.parseInt(getProperty("zookeeper.session.timeout.ms"));
		}

		public int getSyncTimeMs() {
			return Integer.parseInt(getProperty("zookeeper.sync.time.ms"));
		}
	}

}
