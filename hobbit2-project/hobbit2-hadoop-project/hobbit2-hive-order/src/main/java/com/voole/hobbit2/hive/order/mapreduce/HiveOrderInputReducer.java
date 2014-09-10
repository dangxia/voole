/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.hive.order.mapreduce;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroKeyRecordWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import com.google.common.base.Throwables;
import com.voole.dungbeetle.api.DumgBeetleTransformException;
import com.voole.dungbeetle.api.model.HiveTable;
import com.voole.dungbeetle.order.record.OrderDetailDumgBeetleTransformer;
import com.voole.hobbit2.camus.order.OrderPlayAliveReqV2;
import com.voole.hobbit2.camus.order.OrderPlayAliveReqV3;
import com.voole.hobbit2.camus.order.OrderPlayBgnReqV2;
import com.voole.hobbit2.camus.order.OrderPlayBgnReqV3;
import com.voole.hobbit2.camus.order.OrderPlayEndReqV2;
import com.voole.hobbit2.camus.order.OrderPlayEndReqV3;
import com.voole.hobbit2.hive.order.HiveOrderDryRecordGenerator;
import com.voole.hobbit2.hive.order.HiveOrderMetaConfigs;
import com.voole.hobbit2.hive.order.OrderSessionInfo;
import com.voole.hobbit2.hive.order.avro.HiveOrderDryRecord;
import com.voole.hobbit2.hive.order.exception.OrderSessionInfoException;
import com.voole.hobbit2.hive.order.exception.OrderSessionInfoException.OrderSessionInfoExceptionType;

/**
 * @author XuehuiHe
 * @date 2014年7月29日
 */
public class HiveOrderInputReducer extends
		Reducer<Text, AvroValue<SpecificRecordBase>, Object, Object> {
	// private Logger log =
	// LoggerFactory.getLogger(HiveOrderInputReducer.class);
	private OrderSessionInfo sessionInfo;
	private OrderDetailDumgBeetleTransformer orderDetailDumgBeetleTransformer;
	private long currCamusExecTime;

	private LinkedList<SpecificRecordBase> cache;

	private FileSystem fs;
	private Schema errorSchema;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		currCamusExecTime = HiveOrderMetaConfigs.getCurrCamusExecTime(context) / 1000;
		sessionInfo = new OrderSessionInfo();

		orderDetailDumgBeetleTransformer = new OrderDetailDumgBeetleTransformer();
		orderDetailDumgBeetleTransformer.setup(context);

		cache = new LinkedList<SpecificRecordBase>();
		fs = FileSystem.get(context.getConfiguration());
		errorSchema = HiveOrderMetaConfigs.getOrderUnionSchema(context);
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		sessionInfo = null;
		if (orderDetailDumgBeetleTransformer != null) {
			orderDetailDumgBeetleTransformer.cleanup(context);
		}
		cache.clear();
	}

	@Override
	protected void reduce(Text sessionId,
			Iterable<AvroValue<SpecificRecordBase>> iterable, Context context)
			throws IOException, InterruptedException {
		sessionInfo.clear();
		sessionInfo.setSessionId(sessionId.toString());
		cache.clear();

		for (AvroValue<SpecificRecordBase> avroValue : iterable) {
			cache.add(avroValue.datum());
		}

		try {

			for (SpecificRecordBase record : cache) {
				if (record instanceof OrderPlayBgnReqV2) {
					sessionInfo.setBgn((OrderPlayBgnReqV2) record);
				} else if (record instanceof OrderPlayBgnReqV3) {
					sessionInfo.setBgn((OrderPlayBgnReqV3) record);
				} else if (record instanceof OrderPlayEndReqV2) {
					sessionInfo.setEnd((OrderPlayEndReqV2) record);
				} else if (record instanceof OrderPlayEndReqV3) {
					sessionInfo.setEnd((OrderPlayEndReqV3) record);
				} else if (record instanceof OrderPlayAliveReqV2) {
					sessionInfo.setAlive((OrderPlayAliveReqV2) record);
				} else if (record instanceof OrderPlayAliveReqV3) {
					sessionInfo.setAlive((OrderPlayAliveReqV3) record);
				} else {
					throw new UnsupportedOperationException(record.getClass()
							.getName() + " don't support");
				}
			}
			sessionInfo.verify();
			HiveOrderDryRecord orderRecord = HiveOrderDryRecordGenerator
					.generate(sessionInfo);
			if (!isEnd(orderRecord, context)) {
				writeNoEnd(context);
				return;
			}

			Map<HiveTable, List<SpecificRecordBase>> result = orderDetailDumgBeetleTransformer
					.transform(orderRecord);
			if (result != null && result.size() > 0) {
				for (Entry<HiveTable, List<SpecificRecordBase>> entry : result
						.entrySet()) {
					context.write(entry.getKey(), entry.getValue());
				}
			}
		} catch (OrderSessionInfoException e) {
			if (e.getType() == OrderSessionInfoExceptionType.BGN_IS_NULL
					&& isDelayBgn()) {
				writeNoEnd(context);
			} else {
				writeError(e, context);
			}
		} catch (DumgBeetleTransformException e) {
			Throwables.propagate(e);
		}

	}

	public void writeNoEnd(Context context) throws IOException,
			InterruptedException {
		for (SpecificRecordBase record : cache) {
			context.write(NullWritable.get(), record);
		}
	}

	public void writeError(OrderSessionInfoException e, Context context)
			throws IOException, InterruptedException {
		context.getCounter("session_error", e.getType().name()).increment(1l);
		if (e.getDiff() != null) {
			context.getCounter("session_error_diff", e.getType().name()).increment(e.getDiff());
		}
		// RecordWriter<AvroKey<SpecificRecordBase>, NullWritable> errorWriter =
		// createErrorWriter(
		// e, context);
		// AvroKey<SpecificRecordBase> key = new AvroKey<SpecificRecordBase>();
		// for (SpecificRecordBase record : cache) {
		// key.datum(record);
		// errorWriter.write(key, NullWritable.get());
		// }
		// errorWriter.close(context);
	}

	private RecordWriter<AvroKey<SpecificRecordBase>, NullWritable> createErrorWriter(
			OrderSessionInfoException e, Context context) throws IOException {
		String fileName = e.getFileName();
		Path workPath = ((FileOutputCommitter) context.getOutputCommitter())
				.getWorkPath();
		Path path = new Path(workPath, fileName);
		return new AvroKeyRecordWriter<SpecificRecordBase>(errorSchema,
				AvroSerialization.createDataModel(context.getConfiguration()),
				HiveOrderMultiOutputFormat.getCompressionCodec(context),
				fs.create(path),
				HiveOrderMultiOutputFormat.getSyncInterval(context));

	}

	private boolean isDelayBgn() {
		Long last = null;
		if (sessionInfo._end != null) {
			last = sessionInfo._endTime;
		} else if (last == null && sessionInfo._lastAlive != null) {
			last = sessionInfo._lastAliveTime;
		}
		if (last != null) {
			return last > currCamusExecTime - 10 * 60;
		}
		return false;
	}

	Random r = new Random();

	private boolean isEnd(HiveOrderDryRecord orderRecord, Context context) {
		Long last = null;
		if (orderRecord.getPlayEndTime() != null) {
			last = orderRecord.getPlayEndTime();
		} else if (orderRecord.getPlayAliveTime() != null) {
			last = orderRecord.getPlayAliveTime();
		} else {
			last = orderRecord.getPlayBgnTime();
		}
		if (last != null) {
			return last < currCamusExecTime - 10 * 60;
		}
		return true;
	}

}
