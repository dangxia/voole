/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hibbit.storm.kafka.partition;

import java.io.Serializable;

import kafka.api.OffsetRequest;
import backtype.storm.spout.MultiScheme;
import backtype.storm.spout.RawMultiScheme;

/**
 * @author XuehuiHe
 * @date 2014年5月28日
 */
public class KafkaConfig implements Serializable {

	private BrokerHosts hosts;
	private int fetchSizeBytes = 1024 * 1024;
	private int socketTimeoutMs = 10000;
	private int bufferSizeBytes = 1024 * 1024;
	private MultiScheme scheme = new RawMultiScheme();
	private String[] topics;
	private long startOffsetTime;
	private boolean forceFromStart = false;

	public KafkaConfig(BrokerHosts hosts, String... topics) {
		this.hosts = hosts;
		this.topics = topics;
		this.startOffsetTime = OffsetRequest.EarliestTime();
	}

	public void forceStartOffsetTime(long millis) {
		startOffsetTime = millis;
		forceFromStart = true;
	}

	public BrokerHosts getHosts() {
		return hosts;
	}

	public void setHosts(BrokerHosts hosts) {
		this.hosts = hosts;
	}

	public int getFetchSizeBytes() {
		return fetchSizeBytes;
	}

	public void setFetchSizeBytes(int fetchSizeBytes) {
		this.fetchSizeBytes = fetchSizeBytes;
	}

	public int getSocketTimeoutMs() {
		return socketTimeoutMs;
	}

	public void setSocketTimeoutMs(int socketTimeoutMs) {
		this.socketTimeoutMs = socketTimeoutMs;
	}

	public int getBufferSizeBytes() {
		return bufferSizeBytes;
	}

	public void setBufferSizeBytes(int bufferSizeBytes) {
		this.bufferSizeBytes = bufferSizeBytes;
	}

	public MultiScheme getScheme() {
		return scheme;
	}

	public void setScheme(MultiScheme scheme) {
		this.scheme = scheme;
	}

	public String[] getTopics() {
		return topics;
	}

	public void setTopics(String[] topic) {
		this.topics = topic;
	}

	public long getStartOffsetTime() {
		return startOffsetTime;
	}

	public void setStartOffsetTime(long startOffsetTime) {
		this.startOffsetTime = startOffsetTime;
	}

	public boolean isForceFromStart() {
		return forceFromStart;
	}

	public void setForceFromStart(boolean forceFromStart) {
		this.forceFromStart = forceFromStart;
	}

}
