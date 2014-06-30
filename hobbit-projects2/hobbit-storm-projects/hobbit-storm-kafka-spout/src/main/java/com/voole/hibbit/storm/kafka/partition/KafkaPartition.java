/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hibbit.storm.kafka.partition;

import java.io.Serializable;

import storm.trident.spout.ISpoutPartition;

/**
 * @author XuehuiHe
 * @date 2014年5月28日
 */
public class KafkaPartition implements Serializable, ISpoutPartition {
	private HostPort hostport;
	private int partition;
	private String topic;

	public KafkaPartition() {
	}

	public KafkaPartition(HostPort hostport, String topic, int partition) {
		this.hostport = hostport;
		this.partition = partition;
		this.topic = topic;
	}

	public KafkaPartition(String host, int port, String topic, int partition) {
		this(new HostPort(host, port), topic, partition);
	}

	@Override
	public String getId() {
		return hostport.getHost() + ":" + hostport.getPort() + ":" + getTopic()
				+ ":" + +getPartition();
	}

	public HostPort getHostport() {
		return hostport;
	}

	public void setHostport(HostPort hostport) {
		this.hostport = hostport;
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return getHostport().toString() + "\ttopic" + getTopic()
				+ "\tpartition:" + getPartition();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (obj == this) {
			return true;
		}
		if (obj instanceof KafkaPartition) {
			KafkaPartition that = (KafkaPartition) obj;
			return this.getHostport().equals(that.getHostport())
					&& this.getPartition() == that.getPartition()
					&& this.getTopic().equals(that.getTopic());
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return this.getHostport().hashCode() + this.getPartition() * 17
				+ this.getTopic().hashCode() * 7;
	}

}
