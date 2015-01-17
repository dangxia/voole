package com.voole.hobbit2.storm.order2.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPhoenixConnections {
	@BeforeClass
	public static void setup() throws SQLException {
		Connection conn = PhoenixConnections.getConnection();
		Statement statment = conn.createStatement();
		statment.executeUpdate("CREATE TABLE my_table_2 ( id BIGINT not null primary key, date DATE )");
		conn.commit();
	}

	@Test
	public void test() {

	}

	@AfterClass
	public static void clean() throws SQLException {
		// Connection conn = PhoenixConnections.getConnection();
		// conn.createStatement().execute("DROP TABLE my_table_1");
		// conn.commit();
		// PhoenixConnections.shutdown();
	}

	public static void main(String[] args) throws SQLException {
		Connection con = DriverManager
				.getConnection("jdbc:phoenix:data-slave2.voole.com,data-slave3.voole.com,data-slave4.voole.com");

		// con.nativeSQL("CREATE TABLE my_table_1 ( id BIGINT not null primary key, date DATE not null)");
		con.createStatement()
				.executeUpdate(
						"CREATE TABLE my_table_1 ( id BIGINT not null primary key, date DATE )");

		con.commit();
		// con.close();
	}
}
