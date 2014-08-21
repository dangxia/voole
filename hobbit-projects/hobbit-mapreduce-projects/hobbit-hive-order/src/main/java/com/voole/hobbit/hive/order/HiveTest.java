/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.hive.order;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

/**
 * @author XuehuiHe
 * @date 2014年8月14日
 */
public class HiveTest {
	public static void main(String[] args) {
		ClassPathXmlApplicationContext cxt = new ClassPathXmlApplicationContext(
				"hive-db.xml");
		JdbcTemplate hiveClient = cxt.getBean(JdbcTemplate.class);
		String sql = "show tables 'testHiveDriverTable'";
		System.out.println("Running: " + sql);
		hiveClient.query(sql, new RowMapper<Void>() {

			@Override
			public Void mapRow(ResultSet rs, int rowNum) throws SQLException {
				System.out.println(rs.getString(1));
				return null;
			}
		});
		cxt.close();
	}
}
