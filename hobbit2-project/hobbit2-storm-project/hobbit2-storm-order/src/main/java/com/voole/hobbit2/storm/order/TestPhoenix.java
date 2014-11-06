package com.voole.hobbit2.storm.order;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestPhoenix {
	public static void main(String[] args) throws SQLException {
		Statement stmt = null;
		ResultSet rset = null;

		Connection con = DriverManager
				.getConnection("jdbc:phoenix:data-slave2.voole.com,data-slave3.voole.com,data-slave4.voole.com");
		stmt = con.createStatement();

//		stmt.executeUpdate("create table test2 (mykey integer not null primary key, mycolumn varchar)");
		stmt.executeUpdate("upsert into test2 values (1,'Hello')");
		stmt.executeUpdate("upsert into test2 values (2,'World!')");
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
}
