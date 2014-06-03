/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hibbit.storm.kafka.partition;

import java.io.Serializable;

/**
 * @author XuehuiHe
 * @date 2014年5月29日
 */
public interface BrokerHosts extends Serializable {
	public static class ZkHosts implements BrokerHosts {
		private String sessionTimeout;
		private String connectionTimeout;
		private String kafkaConnetion = null;
		private int refreshFreqSecs = 60;

		public ZkHosts() {
		}

		public String getKafkaConnetion() {
			return kafkaConnetion;
		}

		public void setKafkaConnetion(String kafkaConnetion) {
			this.kafkaConnetion = kafkaConnetion;
		}

		public int getRefreshFreqSecs() {
			return refreshFreqSecs;
		}

		public void setRefreshFreqSecs(int refreshFreqSecs) {
			this.refreshFreqSecs = refreshFreqSecs;
		}

		public String getSessionTimeout() {
			return sessionTimeout;
		}

		public void setSessionTimeout(String sessionTimeout) {
			this.sessionTimeout = sessionTimeout;
		}

		public String getConnectionTimeout() {
			return connectionTimeout;
		}

		public void setConnectionTimeout(String connectionTimeout) {
			this.connectionTimeout = connectionTimeout;
		}

	}
}