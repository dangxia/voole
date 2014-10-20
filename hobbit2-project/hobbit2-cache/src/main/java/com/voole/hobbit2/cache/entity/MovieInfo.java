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
public class MovieInfo implements Serializable {
	private static final long serialVersionUID = 6540647247488252639L;
	private Integer dim_cp_id;
	private String category;

	public MovieInfo() {
	}

	public Integer getDim_cp_id() {
		return dim_cp_id;
	}

	public void setDim_cp_id(Integer dim_cp_id) {
		this.dim_cp_id = dim_cp_id;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}
}
