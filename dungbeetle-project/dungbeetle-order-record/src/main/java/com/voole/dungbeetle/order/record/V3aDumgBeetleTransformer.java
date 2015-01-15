/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.dungbeetle.order.record;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hive.service.cli.Type;

import com.google.common.collect.Lists;
import com.voole.dungbeetle.api.DumgBeetleTransformException;
import com.voole.dungbeetle.api.IDumgBeetleTransformer;
import com.voole.dungbeetle.api.model.HiveTable;
import com.voole.dungbeetle.api.model.HiveTablePartition;
import com.voole.dungbeetle.api.model.HiveTableSchema;
import com.voole.dungbeetle.api.model.HiveTableSchema.HiveTablePartitionSchema;
import com.voole.dungbeetle.order.record.avro.V3aLogRecord;
import com.voole.hobbit2.hive.order.avro.V3aLogVersion1;

/**
 * @author XuehuiHe
 * @date 2014年9月6日
 */
public class V3aDumgBeetleTransformer implements
		IDumgBeetleTransformer<V3aLogVersion1> {
	private final Map<String, HiveTable> partitionCache;

	private static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
	private static SimpleDateFormat df2 = new SimpleDateFormat("HH");

	public V3aDumgBeetleTransformer() {
		partitionCache = new HashMap<String, HiveTable>();
	}

	@Override
	public void setup(TaskAttemptContext context) throws IOException,
			InterruptedException {
	}

	@Override
	public void cleanup(TaskAttemptContext context) throws IOException,
			InterruptedException {
	}

	@Override
	public Map<HiveTable, List<SpecificRecordBase>> transform(V3aLogVersion1 dry)
			throws DumgBeetleTransformException {
		V3aLogRecord record = covert(dry);
		String partition = getDayPartition(record.getStamp());
		record.setHour(getDayHour(record.getStamp()));
		Map<HiveTable, List<SpecificRecordBase>> result = new HashMap<HiveTable, List<SpecificRecordBase>>();
		result.put(getTable(partition),
				Lists.newArrayList((SpecificRecordBase) record));
		return result;
	}

	public V3aLogRecord covert(V3aLogVersion1 dry) {
		V3aLogRecord record = new V3aLogRecord();
		record.setHid(dry.getHid());
		record.setOemid(dry.getOemid());
		record.setServerip(dry.getServerip());
		record.setStamp(dry.getStamp());
		record.setUid(dry.getUid());
		record.setUserIp(dry.getUserIp());
		record.setStatus(dry.getStatus());

		cutHidAndToUperCase(record);
		return record;
	}

	protected static void cutHidAndToUperCase(V3aLogRecord record) {
		CharSequence hid = record.getHid();
		if (hid != null && hid.length() > 12) {
			hid = hid.toString().substring(0, 12);
		}
		if (hid != null) {
			record.setHid(hid.toString().toUpperCase());
		}
	}

	private String getDayPartition(long stamp) {
		return df.format(new Date(stamp));
	}

	private String getDayHour(long stamp) {
		return df2.format(new Date(stamp));
	}

	public HiveTable getTable(String partition) {
		if (!partitionCache.containsKey(partition)) {
			createHiveTable(partition);
		}
		return partitionCache.get(partition);
	}

	private synchronized void createHiveTable(String partition) {
		if (partitionCache.containsKey(partition)) {
			return;
		}
		partitionCache.put(partition, HiveTableCreator.create(partition));
	}

	public static class HiveTableCreator {
		public static HiveTable create(String partitionValue) {
			HiveTable table = new HiveTable();
			table.setName("fact_auth");
			table.setSchema(V3aLogRecord.getClassSchema());
			HiveTablePartition partition = new HiveTablePartition();
			partition.setName("day");
			partition.setType(Type.STRING_TYPE);
			partition.setValue(partitionValue);
			table.getPartitions().add(partition);
			return table;
		}

		public static void main(String[] args) {

			HiveTableSchema tableSchema = new HiveTableSchema("fact_auth",
					Lists.newArrayList(new HiveTablePartitionSchema("day",
							Type.STRING_TYPE)), V3aLogRecord.getClassSchema());
			System.out.println(HiveTableSchema
					.getCreateHiveTableSchema(tableSchema));
		}
	}

}
