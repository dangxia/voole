/*
 * Copyright (C) 2013 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.cache.entity;

import java.io.Serializable;

/**
 * 父类信息
 * 
 * @author XuehuiHe
 * @date 2013年12月4日
 */
public class ParentSectionInfo implements Serializable {

	private static final long serialVersionUID = -4992314548930938898L;
	
	private String code;

	public ParentSectionInfo() {
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}


}
