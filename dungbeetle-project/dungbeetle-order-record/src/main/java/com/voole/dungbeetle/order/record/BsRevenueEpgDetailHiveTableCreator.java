package com.voole.dungbeetle.order.record;

import org.apache.hive.service.cli.Type;

import com.google.common.collect.Lists;
import com.voole.dungbeetle.api.model.HiveTable;
import com.voole.dungbeetle.api.model.HiveTablePartition;
import com.voole.dungbeetle.api.model.HiveTableSchema;
import com.voole.dungbeetle.api.model.HiveTableSchema.HiveTablePartitionSchema;
import com.voole.dungbeetle.order.record.avro.BsRevenueDetailInfo;

public class BsRevenueEpgDetailHiveTableCreator {
	public static HiveTable create(String partitionValue) {
		HiveTable table = new HiveTable();
		table.setName("fact_bs_revenue");
		table.setSchema(BsRevenueDetailInfo.getClassSchema());
		HiveTablePartition partition = new HiveTablePartition();
		partition.setName("day");
		partition.setType(Type.STRING_TYPE);
		partition.setValue(partitionValue);
		table.getPartitions().add(partition);
		return table;
	}

	public static void main(String[] args) {

		HiveTableSchema tableSchema = new HiveTableSchema("fact_bs_revenue",
				Lists.newArrayList(new HiveTablePartitionSchema("day",
						Type.STRING_TYPE)),
				BsRevenueDetailInfo.getClassSchema());
		System.out.println(HiveTableSchema
				.getCreateHiveTableSchema(tableSchema));
	}
}
