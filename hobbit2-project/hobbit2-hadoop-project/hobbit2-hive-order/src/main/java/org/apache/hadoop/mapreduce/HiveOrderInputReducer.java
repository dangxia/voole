/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.avro.mapred.AvroValue;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ReduceContext.ValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	private Logger log = LoggerFactory.getLogger(HiveOrderInputReducer.class);
	private OrderSessionInfo sessionInfo;
	private OrderDetailDumgBeetleTransformer orderDetailDumgBeetleTransformer;
	private long currCamusExecTime;
	private long total = 0l;
	private long noendTotal = 0l;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		currCamusExecTime = HiveOrderMetaConfigs.getCurrCamusExecTime(context) / 1000;
		sessionInfo = new OrderSessionInfo();

		orderDetailDumgBeetleTransformer = new OrderDetailDumgBeetleTransformer();
		orderDetailDumgBeetleTransformer.setup(context);
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		sessionInfo = null;
		if (orderDetailDumgBeetleTransformer != null) {
			orderDetailDumgBeetleTransformer.cleanup(context);
		}
	}

	@Override
	protected void reduce(Text sessionId,
			Iterable<AvroValue<SpecificRecordBase>> iterable, Context context)
			throws IOException, InterruptedException {
		sessionInfo.clear();
		sessionInfo.setSessionId(sessionId.toString());

		total = 0l;
		noendTotal = 0l;
		ValueIterator<AvroValue<SpecificRecordBase>> iterator = (ValueIterator<AvroValue<SpecificRecordBase>>) iterable
				.iterator();
		try {
			iterator.mark();
			while (iterator.hasNext()) {
				total++;
				AvroValue<SpecificRecordBase> avroValue = iterator.next();
				SpecificRecordBase record = avroValue.datum();
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
				writeNoEnd(iterator, context);
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
				writeNoEnd(iterator, context);
			} else {
				writeError(e, iterable, context);
			}
		} catch (DumgBeetleTransformException e) {
			Throwables.propagate(e);
		} finally {
			iterator.resetBackupStore();
		}

	}

	public void writeNoEnd(
			MarkableIteratorInterface<AvroValue<SpecificRecordBase>> iterator,
			Context context) throws IOException, InterruptedException {
		log.info("write no end");
		iterator.reset();
		while (iterator.hasNext()) {
			noendTotal++;
			AvroValue<SpecificRecordBase> avroValue = iterator.next();
			context.write(NullWritable.get(), avroValue.datum());
		}
		log.info(sessionInfo.getSessionId() + "total:" + total
				+ "\tnoendTotal:" + noendTotal);
	}

	public void writeError(OrderSessionInfoException e,
			Iterable<AvroValue<SpecificRecordBase>> iterable, Context context)
			throws IOException, InterruptedException {
		// TODO
		// for (AvroValue<SpecificRecordBase> avroValue : iterable) {
		// context.write(e, avroValue.datum());
		// }
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
			// return last%2==0;
			// return last < currCamusExecTime - 10 * 60;
		}
		return r.nextBoolean();
		// return true;
	}

}
