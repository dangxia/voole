/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * @author XuehuiHe
 * @date 2014年9月25日
 */
public class TestHbase {
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();

		// HTable table = new HTable(conf, "test_hbase2");
		// Put put = new Put("key1".getBytes());
		// put.add("cf".getBytes(), "c1".getBytes(), 1,
		// "c1_value_2".getBytes());
		// put.add("cf".getBytes(), "c2".getBytes(), 1,
		// "c2_value_1".getBytes());
		// table.put(put);
		// table.close();
		// put.add("cf".get, qualifier, ts, value)
		HBaseAdmin admin = new HBaseAdmin(conf);
		String tableName = "storm_order_session";
		HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
		HColumnDescriptor cf = new HColumnDescriptor("cf");
		cf.setTimeToLive(3 * 60 * 60);
		cf.setInMemory(true);
		cf.setKeepDeletedCells(false);
		tableDescriptor.addFamily(cf);
		admin.createTable(tableDescriptor);
		admin.close();
	}
}
