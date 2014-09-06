/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.dungbeetle.order.record;

import java.util.ArrayList;
import java.util.List;

import org.apache.hive.service.cli.Type;

import com.voole.dungbeetle.api.model.HiveTable;
import com.voole.dungbeetle.api.model.HiveTablePartition;
import com.voole.dungbeetle.order.record.avro.HiveOrderDetailRecord;

/**
 * @author XuehuiHe
 * @date 2014年9月6日
 */
public class OrderDetailHiveTable extends HiveTable {

	private static class OrderDetailHiveTableHolder {
		private static OrderDetailHiveTable _instance = new OrderDetailHiveTable();
	}

	public static final List<HiveTablePartition> PARTITIONS = new ArrayList<HiveTablePartition>();
	static {
		HiveTablePartition partition = new HiveTablePartition("day",
				Type.STRING_TYPE);
		PARTITIONS.add(partition);
	}

	private OrderDetailHiveTable() {
		super("order_detail_record", PARTITIONS, HiveOrderDetailRecord
				.getClassSchema());
	}

	public static OrderDetailHiveTable get() {
		return OrderDetailHiveTableHolder._instance;
	}

}
