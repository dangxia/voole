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

public class TestPhoenix {
	private static Logger log = LoggerFactory.getLogger(TestPhoenix.class);
	public static void main(String[] args) throws SQLException {
		
		log.info("sjdlfjlsjdlfjsjdlfjlsjldj");
		
		Connection con = DriverManager
				.getConnection("jdbc:phoenix:data-slave2.voole.com,data-slave3.voole.com,data-slave4.voole.com");

		Statement stmt = null;
		ResultSet rset = null;

		stmt = con.createStatement();

		stmt.executeUpdate("create table test3 (mykey integer not null primary key, mycolumn varchar,n bigint)");
		
		con.commit();
		

		PreparedStatement preparedStatement = con
				.prepareStatement("upsert into test3 values (?,?,?)");

		preparedStatement.setInt(1, 2);
		preparedStatement.setString(2, null);
		preparedStatement.setNull(3, java.sql.Types.BIGINT);
		// preparedStatement.setBoolean(parameterIndex, x);
		// preparedStatement.setLong(parameterIndex, x);
		// preparedStatement.setDouble(parameterIndex, x);
		// preparedStatement.setFloat(parameterIndex, x);

		preparedStatement.addBatch();

		preparedStatement.executeBatch();

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
