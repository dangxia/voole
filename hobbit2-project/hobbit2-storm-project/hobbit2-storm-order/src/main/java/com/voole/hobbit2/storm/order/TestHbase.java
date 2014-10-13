/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

/**
 * @author XuehuiHe
 * @date 2014年9月25日
 */
public class TestHbase {
	public static void dsd(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "mytable");
		long ts = System.currentTimeMillis();
		Put put = new Put("row1".getBytes());
		put.add("cf".getBytes(), "c1".getBytes(), ts,
				("clast_" + ts).getBytes());
		// put.add("cf".getBytes(), "c2".getBytes(), ts,
		// ("c2_" + (ts + 5000)).getBytes());
		//
		// table.put(put);
		// put = new Put("row1".getBytes());
		// ts += 5000l;
		// put.add("cf".getBytes(), "c1".getBytes(), ts, ("c1_" +
		// ts).getBytes());
		// put.add("cf".getBytes(), "c2".getBytes(), ts,
		// ("c2_" + (ts + 5000)).getBytes());
		table.put(put);
		table.close();
	}

	public static void main(String[] args) throws MasterNotRunningException,
			ZooKeeperConnectionException, IOException {
		createOrderSessionTable();
		createOnline();
		createOnlineSnapshoot();
	}

	public static void createOnlineSnapshoot()
			throws MasterNotRunningException, ZooKeeperConnectionException,
			IOException {
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		TableName tableName = TableName.valueOf("storm_online_user_snapshoot");
		HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
		HColumnDescriptor cf = new HColumnDescriptor("cf");
		cf.setMaxVersions(1);
		cf.setKeepDeletedCells(false);
		tableDescriptor.addFamily(cf);
		admin.createTable(tableDescriptor);
		admin.close();
	}

	public static void createOnline() throws MasterNotRunningException,
			ZooKeeperConnectionException, IOException {
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		TableName tableName = TableName.valueOf("storm_online_user");
		HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
		HColumnDescriptor cf = new HColumnDescriptor("cf");
		cf.setMaxVersions(1);
		cf.setKeepDeletedCells(false);
		tableDescriptor.addFamily(cf);
		admin.createTable(tableDescriptor);
		admin.close();
	}

	public static void createOrderSessionTable()
			throws MasterNotRunningException, ZooKeeperConnectionException,
			IOException {
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		TableName tableName = TableName.valueOf("storm_order_session");
		HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
		HColumnDescriptor cf = new HColumnDescriptor("cf");
		cf.setTimeToLive(3 * 60 * 60);
		cf.setMaxVersions(1);
		cf.setKeepDeletedCells(false);
		tableDescriptor.addFamily(cf);
		admin.createTable(tableDescriptor);
		admin.close();
	}

}
