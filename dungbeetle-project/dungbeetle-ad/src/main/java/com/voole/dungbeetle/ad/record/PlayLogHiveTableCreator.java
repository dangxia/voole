package com.voole.dungbeetle.ad.record;

import org.apache.hive.service.cli.Type;

import com.voole.dungbeetle.ad.record.avro.PlayLog;
import com.voole.dungbeetle.api.model.HiveTable;
import com.voole.dungbeetle.api.model.HiveTablePartition;

public class PlayLogHiveTableCreator {
	public static HiveTable create(String partitionValue) {
		HiveTable table = new HiveTable();
		table.setName("play_log");
		table.setSchema(PlayLog.getClassSchema());
		HiveTablePartition partition = new HiveTablePartition();
		partition.setName("day");
		partition.setType(Type.STRING_TYPE);
		partition.setValue(partitionValue);
		table.getPartitions().add(partition);
		return table;
	}
}
