/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.tools.kafka.partition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Objects;

/**
 * @author XuehuiHe
 * @date 2014年8月21日
 */
public class Broker implements Serializable, Writable, Comparable<Broker> {
	private String host;
	private int port;
	private int id;

	public Broker() {
	}

	public Broker(String host, int port, int id) {
		this.host = host;
		this.port = port;
		this.id = id;
	}

	public int id() {
		return id;
	}

	public String host() {
		return host;
	}

	public int port() {
		return port;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public int getId() {
		return id;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setId(int id) {
		this.id = id;
	}

	@Override
	public int hashCode() {
		return this.getId();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (this == obj) {
			return true;
		}
		if (obj instanceof Broker) {
			Broker that = (Broker) obj;
			return this.getId() == that.getId();
		}
		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("host", host).add("port", port)
				.add("id", id).toString();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, getHost());
		WritableUtils.writeVInt(out, getPort());
		WritableUtils.writeVInt(out, getId());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.host = WritableUtils.readString(in);
		this.port = WritableUtils.readVInt(in);
		this.id = WritableUtils.readVInt(in);
	}

	@Override
	public int compareTo(Broker o) {
		return this.id - o.id;
	}

}
