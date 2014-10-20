/*
 * Copyright (C) 2013 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.cache.entity;

import java.io.Serializable;

/**
 * 介质信息
 * 
 * @author XuehuiHe
 * @date 2013年12月4日
 */
public class ParentAreaInfo implements Serializable {
	private static final long serialVersionUID = 6540647247488252639L;
	private Integer parentid;

	public ParentAreaInfo() {
	}

	public Integer getParentid() {
		return parentid;
	}

	public void setParentid(Integer parentid) {
		this.parentid = parentid;
	}
}
