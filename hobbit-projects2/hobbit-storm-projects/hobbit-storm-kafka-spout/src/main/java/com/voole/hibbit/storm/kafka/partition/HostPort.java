/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hibbit.storm.kafka.partition;

import java.io.Serializable;

/**
 * @author XuehuiHe
 * @date 2014年5月28日
 */
public class HostPort implements Serializable {
	private String host;
	private int port;

	public HostPort() {
	}

	public HostPort(String host, int port) {
		this.host = host;
		this.port = port;
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "host:" + this.getHost() + "\tport:" + getPort();
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
		if (obj instanceof HostPort) {
			HostPort that = (HostPort) obj;
			return this.getHost().equals(that.getHost())
					&& this.getPort() == that.getPort();
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
		return getHost().hashCode() * 13 + getPort();
	}

}
