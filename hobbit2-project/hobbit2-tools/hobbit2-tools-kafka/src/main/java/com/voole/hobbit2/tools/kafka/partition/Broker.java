/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.tools.kafka.partition;

import java.io.Serializable;

import com.google.common.base.Objects;

/**
 * @author XuehuiHe
 * @date 2014年8月21日
 */
public class Broker implements Serializable {
	private final String host;
	private final int port;
	private final int id;

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

}
