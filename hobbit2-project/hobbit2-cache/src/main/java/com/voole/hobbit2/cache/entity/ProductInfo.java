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
public class ProductInfo implements Serializable {

	private static final long serialVersionUID = -2887855100587147215L;
	private Integer ptype;// 0 100 200 300
	private Integer fee;// 单价
	private Integer pptype;// 0 免费 1 单点 2 订购

	public ProductInfo() {
	}

	public Integer getPtype() {
		return ptype;
	}

	public void setPtype(Integer ptype) {
		this.ptype = ptype;
	}

	public Integer getFee() {
		return fee;
	}

	public void setFee(Integer fee) {
		this.fee = fee;
	}

	public Integer getPptype() {
		return pptype;
	}

	public void setPptype(Integer pptype) {
		this.pptype = pptype;
	}

	public void _process() {
		if (fee == 0) {
			this.setPptype(0);
		} else if (fee > 0 && ptype <= 100) {
			this.setPptype(1);
		} else if (fee > 0 && ptype >= 200) {
			this.setPptype(2);
		}
	}
}
