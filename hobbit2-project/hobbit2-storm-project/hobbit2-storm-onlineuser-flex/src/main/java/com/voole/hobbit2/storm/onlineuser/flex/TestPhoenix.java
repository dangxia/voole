package com.voole.hobbit2.storm.onlineuser.flex;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TestPhoenix {

	public static long twoHourAgo() {
		return System.currentTimeMillis() / 1000 - 60 * 60 * 2;
	}

	public static void main(String[] args) throws ClassNotFoundException {

		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			// Class.forName("org.apache.phoenix.jdbc.PhoenixDriver", true,
			// TestPhoenix.class.getClassLoader());
//			connection = DriverManager
//					.getConnection("jdbc:phoenix:data-slave2.voole.com,data-slave3.voole.com,data-slave4.voole.com,data-slave5.voole.com,data-slave6.voole.com,data-slave7.voole.com");
			connection = DriverManager
					.getConnection("jdbc:phoenix:125.39.216.198,125.39.216.199,125.39.216.200:2181");
			ps = connection
					.prepareStatement("select METRIC_PLAYBGNTIME from FACT_VOD_HISTORY  limit 50 ");
			// where METRIC_PLAYBGNTIME>=? and dim_oem_id>=?
			// ps.setLong(1, twoHourAgo());
			// ps.setInt(2, 705);

			rs = ps.executeQuery();

			long total = 0l;
			while (rs.next()) {
				rs.getLong(1);
				total++;
				System.out.println(total);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {

			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}

		}

	}
}
