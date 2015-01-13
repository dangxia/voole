/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.hive.direct.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.mapred.AvroValue;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.voole.dungbeetle.api.DumgBeetleTransformException;
import com.voole.dungbeetle.api.model.HiveTable;
import com.voole.dungbeetle.order.record.BsPvEpgDetailDumgBeetleTransformer;
import com.voole.dungbeetle.order.record.BsRevenueEpgDetailDumgBeetleTransformer;
import com.voole.dungbeetle.order.record.V3aDumgBeetleTransformer;
import com.voole.hobbit2.hive.order.avro.BsPvPlayDryInfo;
import com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo;
import com.voole.hobbit2.hive.order.avro.V3aLogVersion1;

/**
 * @author XuehuiHe
 * @date 2014年7月29日
 */
public class HiveOrderInputReducer extends
		Reducer<NullWritable, AvroValue<SpecificRecordBase>, Object, Object> {

	private BsPvEpgDetailDumgBeetleTransformer bsPvEpgDetailDumgBeetleTransformer;
	private BsRevenueEpgDetailDumgBeetleTransformer bsRevenueEpgDetailDumgBeetleTransformer;
	private V3aDumgBeetleTransformer v3aDumgBeetleTransformer;

	public HiveOrderInputReducer() {
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		bsPvEpgDetailDumgBeetleTransformer = new BsPvEpgDetailDumgBeetleTransformer();
		bsPvEpgDetailDumgBeetleTransformer.setup(context);

		bsRevenueEpgDetailDumgBeetleTransformer = new BsRevenueEpgDetailDumgBeetleTransformer();
		bsRevenueEpgDetailDumgBeetleTransformer.setup(context);
		
		v3aDumgBeetleTransformer = new V3aDumgBeetleTransformer();
		v3aDumgBeetleTransformer.setup(context);

	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		bsPvEpgDetailDumgBeetleTransformer.cleanup(context);
		bsRevenueEpgDetailDumgBeetleTransformer.cleanup(context);
		v3aDumgBeetleTransformer.cleanup(context);
	}

	@Override
	protected void reduce(NullWritable key,
			Iterable<AvroValue<SpecificRecordBase>> iterable, Context context)
			throws IOException, InterruptedException {

		try {
			for (AvroValue<SpecificRecordBase> avroValue : iterable) {
				SpecificRecordBase record = avroValue.datum();
				Map<HiveTable, List<SpecificRecordBase>> result = null;
				if (record instanceof BsPvPlayDryInfo) {
					result = bsPvEpgDetailDumgBeetleTransformer
							.transform((BsPvPlayDryInfo) record);
				} else if (record instanceof BsRevenueDryInfo) {
					result = bsRevenueEpgDetailDumgBeetleTransformer
							.transform((BsRevenueDryInfo) record);
				} else if (record instanceof V3aLogVersion1) {
					result = v3aDumgBeetleTransformer
							.transform((V3aLogVersion1) record);
				}
				if (result != null && result.size() > 0) {
					for (Entry<HiveTable, List<SpecificRecordBase>> entry : result
							.entrySet()) {
						context.write(entry.getKey(), entry.getValue());
					}
				}
			}
		} catch (DumgBeetleTransformException e) {
			// write order detail TransformException
			context.write("direct_hive_tr", e.getMessage());
		}
	}

}
