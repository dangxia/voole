/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.spring;

import org.springframework.jdbc.core.JdbcTemplate;

/**
 * @author XuehuiHe
 * @date 2014年9月26日
 */
public class TestBean {
	private JdbcTemplate realtimeJt;

	public JdbcTemplate getRealtimeJt() {
		return realtimeJt;
	}

	public void setRealtimeJt(JdbcTemplate realtimeJt) {
		this.realtimeJt = realtimeJt;
	}

}
