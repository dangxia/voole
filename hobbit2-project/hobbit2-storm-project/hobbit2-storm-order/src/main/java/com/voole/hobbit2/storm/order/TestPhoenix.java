package com.voole.hobbit2.storm.order;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import com.voole.hobbit2.common.Hobbit2Utils;

public class TestPhoenix {
	private static Logger log = LoggerFactory.getLogger(TestPhoenix.class);

	public static void main(String[] args) {
		System.out.println(Hobbit2Utils.longToIp(1908947688l));
		System.out.println(Hobbit2Utils.longToIp(2059194600l));
		System.out.println(Hobbit2Utils.longToIp(2099769230l));
		System.out.println(Hobbit2Utils.longToIp(3708493218l));
	}

	public static void main100(String[] args) throws SQLException,
			ClassNotFoundException {

		log.info("sjdlfjlsjdlfjsjdlfjlsjldj");
		Connection con = DriverManager
				.getConnection("jdbc:phoenix:data-slave2.voole.com,data-slave3.voole.com,data-slave4.voole.com");

		PreparedStatement ps1 = con
				.prepareStatement("upsert into test3 (mykey,mycolumn)values (?,?)");
		PreparedStatement ps2 = con
				.prepareStatement("upsert into test3 (mykey,n)values (?,?)");
		ps1.setInt(1, 3);
		ps1.setString(2, "test_3");
		ps1.addBatch();

		// ps2.setInt(1, 3);
		// ps2.setLong(2, 1000l);
		// ps2.addBatch();
		int[] results = ps1.executeBatch();
		for (int i = 0; i < results.length; i++) {
			System.out.println(results[i]);
		}
		results = ps2.executeBatch();
		for (int i = 0; i < results.length; i++) {
			System.out.println(results[i]);
		}

		con.commit();
		con.close();

	}

	public static void main2(String[] args) throws SQLException {

		// ClassPathXmlApplicationContext cxt = new
		// ClassPathXmlApplicationContext(
		// "phoenix-db.xml");
		// JdbcTemplate jdbcTemplate = (JdbcTemplate) cxt
		// .getBean("phoenixTemplate");
		// queryAll(jdbcTemplate);
		//
		// jdbcTemplate.update("upsert into test2 values (1,'Hello2')");
		//
		// jdbcTemplate.batchUpdate("upsert into test2 values (?,?)", new
		// BatchPreparedStatementSetter() {
		//
		// @Override
		// public void setValues(PreparedStatement ps, int i) throws
		// SQLException {
		// ps.setInt(1, 1);
		// ps.setString(2, "ssdf");
		// }
		//
		// @Override
		// public int getBatchSize() {
		// return 1;
		// }
		// });
		//
		// jdbcTemplate.update("upsert into test2 values (?,?)",
		// new PreparedStatementSetter() {
		//
		// @Override
		// public void setValues(PreparedStatement ps)
		// throws SQLException {
		// ps.setInt(1, 1);
		// ps.setString(2, "ssdf");
		// }
		// });
		// queryAll(jdbcTemplate);
		// DriverManager.get
		// // cxt.close();

		Statement stmt = null;
		ResultSet rset = null;

		Connection con = DriverManager
				.getConnection("jdbc:phoenix:data-slave2.voole.com,data-slave3.voole.com,data-slave4.voole.com");
		stmt = con.createStatement();

		stmt.addBatch("upsert into test2 values (1,'Hello222')");
		stmt.addBatch("upsert into test2 values (3,'Hello222')");
		//
		int[] recordsAffected = stmt.executeBatch();
		System.out.println(recordsAffected);
		// stmt.executeUpdate("create table test2 (mykey integer not null primary key, mycolumn varchar)");
		// stmt.executeUpdate("upsert into test2 values (1,'Hello')");
		// stmt.executeUpdate("upsert into test2 values (2,'World!22')");

		con.commit();

		PreparedStatement statement = con
				.prepareStatement("select * from test2");
		rset = statement.executeQuery();
		while (rset.next()) {
			System.out.println(rset.getString("mycolumn"));
		}
		statement.close();
		con.close();
	}

	public static void queryAll(JdbcTemplate jdbcTemplate) {
		jdbcTemplate.query("select * from test2", new RowMapper<Void>() {

			@Override
			public Void mapRow(ResultSet rs, int rowNum) throws SQLException {
				System.out.println(rs.getString("mycolumn"));
				return null;
			}
		});
	}
}
