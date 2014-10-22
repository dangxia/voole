package com.voole.dungbeetle.ad.record;

import org.apache.hive.service.cli.Type;

import com.google.common.collect.Lists;
import com.voole.dungbeetle.ad.record.avro.PlayLog;
import com.voole.dungbeetle.api.model.HiveTable;
import com.voole.dungbeetle.api.model.HiveTablePartition;
import com.voole.dungbeetle.api.model.HiveTableSchema;
import com.voole.dungbeetle.api.model.HiveTableSchema.HiveTablePartitionSchema;

public class OrderDetailHiveTableCreator {
	public static HiveTable create(String partitionValue) {
		HiveTable table = new HiveTable();
		table.setName("fact_vod");
		table.setSchema(PlayLog.getClassSchema());
		HiveTablePartition partition = new HiveTablePartition();
		partition.setName("day");
		partition.setType(Type.STRING_TYPE);
		partition.setValue(partitionValue);
		table.getPartitions().add(partition);
		return table;
	}

	public static void main(String[] args) {

		HiveTableSchema tableSchema = new HiveTableSchema("play_log",
				Lists.newArrayList(new HiveTablePartitionSchema("day",
						Type.STRING_TYPE)), PlayLog.getClassSchema());
		System.out.println(HiveTableSchema
				.getCreateHiveTableSchema(tableSchema));
	}
}
