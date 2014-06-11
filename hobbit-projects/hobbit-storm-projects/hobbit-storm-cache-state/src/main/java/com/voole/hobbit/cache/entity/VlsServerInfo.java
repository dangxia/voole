/*
 * Copyright (C) 2013 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.cache.entity;

import java.io.Serializable;

/**
 * @author XuehuiHe
 * @date 2013年11月19日
 */
public class VlsServerInfo implements Serializable {
	private static final long serialVersionUID = 3459688917908470569L;
	private Integer id;
	private String name;
	private String ip;
	private VlsServerType type;

	public VlsServerInfo() {
	}

	public Integer getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public String getIp() {
		return ip;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public VlsServerType getType() {
		return type;
	}

	public void setType(VlsServerType type) {
		this.type = type;
	}

	public enum VlsServerType {
		SUPER_NODE("超级节点", 0), TRANSFOR_NODE("中转节点", 1), ENDING_NODE("边缘节点", 2);

		private String name;
		private Integer type;

		private VlsServerType(String name, Integer type) {
			this.name = name;
			this.type = type;
		}

		public String getName() {
			return name;
		}

		public Integer getType() {
			return type;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setType(Integer type) {
			this.type = type;
		}

		public static VlsServerType getType(int type) {
			switch (type) {
			case 0:
				return SUPER_NODE;
			case 1:
				return TRANSFOR_NODE;
			case 2:
				return ENDING_NODE;
			default:
				return null;
			}
		}
	}

}
