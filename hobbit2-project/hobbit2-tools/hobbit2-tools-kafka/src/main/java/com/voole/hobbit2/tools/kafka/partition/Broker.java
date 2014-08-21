/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.tools.kafka.partition;

import java.io.Serializable;

import com.voole.hobbit2.tools.common.Hobbi2tUtils;

/**
 * @author XuehuiHe
 * @date 2014年8月21日
 */
public class Broker implements Serializable {
	private String host;
	private int port;
	private int id;

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

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getId() {
		return id;
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
			return Hobbi2tUtils.equals(this.getHost(), that.getHost())
					&& Hobbi2tUtils.equals(this.getId(), that.getId())
					&& Hobbi2tUtils.equals(this.getPort(), that.getPort());
		}
		return false;
	}

	@Override
	public String toString() {
		return "host:" + Hobbi2tUtils.toString(this.getHost()) + ",\tport:"
				+ Hobbi2tUtils.toString(this.getPort()) + "\tid:"
				+ Hobbi2tUtils.toString(this.getId());
	}

}
