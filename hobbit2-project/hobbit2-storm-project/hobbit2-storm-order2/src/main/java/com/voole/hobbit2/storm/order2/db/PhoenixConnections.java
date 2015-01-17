package com.voole.hobbit2.storm.order2.db;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.dbcp.BasicDataSource;

public class PhoenixConnections {

	private static class DataSourceHolder {
		private static final BasicDataSource _instance = createDataSource();
	}

	public static Connection getConnection() throws SQLException {
		return DataSourceHolder._instance.getConnection();
	}

	private static BasicDataSource createDataSource() {
		BasicDataSource dataSource = new BasicDataSource();
		dataSource.setDefaultAutoCommit(false);
		dataSource
				.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
		dataSource.setDriverClassName("org.apache.phoenix.jdbc.PhoenixDriver");
		dataSource.setInitialSize(4);
		dataSource.setMaxActive(12);
		dataSource.setMaxIdle(12);
		// dataSource.setDefaultTransactionIsolation(defaultTransactionIsolation);
		// dataSource.setMaxWait(-1);
		dataSource
				.setUrl("jdbc:phoenix:data-slave2.voole.com,data-slave3.voole.com,data-slave4.voole.com");
		return dataSource;
	}

	public static void shutdown() throws SQLException {
		DataSourceHolder._instance.close();
	}

}
