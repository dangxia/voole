package com.voole.dungbeetle.order.record;

import org.apache.hive.service.cli.Type;

import com.voole.dungbeetle.api.model.HiveTable;
import com.voole.dungbeetle.api.model.HiveTablePartition;
import com.voole.dungbeetle.order.record.avro.HiveOrderDetailRecord;

public class OrderDetailHiveTableCreator {
	public static HiveTable create(String partitionValue) {
		HiveTable table = new HiveTable();
		table.setName("order_detail_record");
		table.setSchema(HiveOrderDetailRecord.getClassSchema());
		HiveTablePartition partition = new HiveTablePartition();
		partition.setName("day");
		partition.setType(Type.STRING_TYPE);
		partition.setValue(partitionValue);
		table.getPartitions().add(partition);
		return table;
	}
}
