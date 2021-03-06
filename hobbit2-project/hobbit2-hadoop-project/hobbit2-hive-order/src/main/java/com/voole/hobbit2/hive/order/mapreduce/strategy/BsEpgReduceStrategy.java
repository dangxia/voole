package com.voole.hobbit2.hive.order.mapreduce.strategy;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.data.RecordBuilder;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voole.dungbeetle.api.DumgBeetleTransformException;
import com.voole.dungbeetle.api.model.HiveTable;
import com.voole.dungbeetle.order.record.OrderDetailDumgBeetleTransformer;
import com.voole.hobbit2.camus.bsepg.BsEpgPlayInfo;
import com.voole.hobbit2.hive.order.avro.HiveOrderDryRecord;
import com.voole.hobbit2.hive.order.mapreduce.HiveOrderInputReducer;
import com.voole.hobbit2.order.common.BsEpgHiveOrderDryRecordGenerator;
import com.voole.hobbit2.order.common.BsEpgOrderSessionInfo;

public class BsEpgReduceStrategy {

	private static final Logger LOG = LoggerFactory
			.getLogger(BsEpgReduceStrategy.class);
	private OrderDetailDumgBeetleTransformer orderDetailDumgBeetleTransformer;
	private long currCamusExecTime;
	private final BsEpgOrderSessionInfo sessionInfo;

	private final Map<Class<?>, Method> clazzToCreateBuilderMethod;

	public BsEpgReduceStrategy() {
		sessionInfo = new BsEpgOrderSessionInfo();
		clazzToCreateBuilderMethod = new HashMap<Class<?>, Method>();
	}

	public void reduce(Text sessionIdAndNatip,
			Iterable<AvroValue<SpecificRecordBase>> iterable, Context context)
			throws IOException, InterruptedException {
		sessionInfo.clear();

		try {
			for (AvroValue<SpecificRecordBase> avroValue : iterable) {
				SpecificRecordBase record = avroValue.datum();
				if (record instanceof BsEpgPlayInfo) {
					sessionInfo.setPlayInfo(deepCopy((BsEpgPlayInfo) record));
				} else {
					throw new UnsupportedOperationException(record.getClass()
							.getName() + " don't support");
				}
			}
			boolean hasBgn = sessionInfo.get_bgn() != null;
			boolean hasEnd = sessionInfo.get_end() != null;
			if (hasBgn)
				context.getCounter("epg_play_info", "has_bgn").increment(1l);
			if (hasEnd)
				context.getCounter("epg_play_info", "has_end").increment(1l);
			if (hasBgn && hasEnd)
				context.getCounter("epg_play_info", "has_full").increment(1l);

			if (!sessionInfo.isEnd(currCamusExecTime)) {
				writeNoEnd(context);
				return;
			}
			HiveOrderDryRecord orderRecord = BsEpgHiveOrderDryRecordGenerator
					.generate(sessionInfo);

//			if (hasBgn && hasEnd) {
//				LOG.info("BGN:" + sessionInfo.get_bgn());
//				LOG.info("END:" + sessionInfo.get_end());
//				LOG.info("DRY:" + orderRecord);
//			}

			Map<HiveTable, List<SpecificRecordBase>> result = orderDetailDumgBeetleTransformer
					.transform(orderRecord);
			if (result != null && result.size() > 0) {
				for (Entry<HiveTable, List<SpecificRecordBase>> entry : result
						.entrySet()) {
					context.write(entry.getKey(), entry.getValue());
				}
			}
		} catch (DumgBeetleTransformException e) {
			// write order detail TransformException
			context.write(
					HiveOrderInputReducer.ORDER_DETAIL_TRANSFORM_EXCEPTION,
					e.getMessage());
		} catch (Exception e) {
			context.write(
					HiveOrderInputReducer.ORDER_DETAIL_TRANSFORM_EXCEPTION,
					e.getMessage());
		}

	}

	protected <T extends SpecificRecordBase> T deepCopy(T record)
			throws IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, NoSuchMethodException, SecurityException {
		RecordBuilder<?> builder = (RecordBuilder<?>) getBuilderMethod(record)
				.invoke(null, record);
		return (T) builder.build();
	}

	protected Method getBuilderMethod(SpecificRecordBase record)
			throws NoSuchMethodException, SecurityException {
		Class<?> clazz = record.getClass();
		if (!clazzToCreateBuilderMethod.containsKey(clazz)) {
			clazzToCreateBuilderMethod.put(clazz,
					clazz.getMethod("newBuilder", clazz));
		}
		return clazzToCreateBuilderMethod.get(clazz);
	}

	public void writeNoEnd(Context context) throws IOException,
			InterruptedException {
		if (sessionInfo.get_bgn() != null) {
			context.write(NullWritable.get(), sessionInfo.get_bgn());
		}
	}

	public OrderDetailDumgBeetleTransformer getOrderDetailDumgBeetleTransformer() {
		return orderDetailDumgBeetleTransformer;
	}

	public void setOrderDetailDumgBeetleTransformer(
			OrderDetailDumgBeetleTransformer orderDetailDumgBeetleTransformer) {
		this.orderDetailDumgBeetleTransformer = orderDetailDumgBeetleTransformer;
	}

	public long getCurrCamusExecTime() {
		return currCamusExecTime;
	}

	public void setCurrCamusExecTime(long currCamusExecTime) {
		this.currCamusExecTime = currCamusExecTime;
	}

}
