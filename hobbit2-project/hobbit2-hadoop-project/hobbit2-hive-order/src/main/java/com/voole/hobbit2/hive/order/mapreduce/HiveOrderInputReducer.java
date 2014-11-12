/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.hive.order.mapreduce;

import java.io.IOException;

import org.apache.avro.mapred.AvroValue;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.voole.dungbeetle.ad.transform.AdPlayLogTransformerImpl;
import com.voole.dungbeetle.order.record.OrderDetailDumgBeetleTransformer;
import com.voole.hobbit2.hive.order.HiveOrderMetaConfigs;
import com.voole.hobbit2.hive.order.mapreduce.strategy.BsEpgReduceStrategy;
import com.voole.hobbit2.hive.order.mapreduce.strategy.CtypeReduceStrategy;

/**
 * @author XuehuiHe
 * @date 2014年7月29日
 */
public class HiveOrderInputReducer extends
		Reducer<Text, AvroValue<SpecificRecordBase>, Object, Object> {
	// private Logger log =
	// LoggerFactory.getLogger(HiveOrderInputReducer.class);
	private OrderDetailDumgBeetleTransformer orderDetailDumgBeetleTransformer;
	private AdPlayLogTransformerImpl adPlayLogTransformerImpl;

	private BsEpgReduceStrategy bsEpgReduceStrategy;
	private CtypeReduceStrategy ctypeReduceStrategy;

	public static final String ORDER_DETAIL_TRANSFORM_EXCEPTION = "order_detail_transform_exception";
	public static final String AD_TRANSFORM_EXCEPTION = "ad_transform_exception";

	public HiveOrderInputReducer() {
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		long currCamusExecTime = HiveOrderMetaConfigs
				.getCurrCamusExecTime(context) / 1000;

		orderDetailDumgBeetleTransformer = new OrderDetailDumgBeetleTransformer();
		orderDetailDumgBeetleTransformer.setup(context);

		boolean isRunadPlayLogTransformer = HiveOrderMetaConfigs
				.isRunadPlayLogTransformer(context);
		if (isRunadPlayLogTransformer) {
			adPlayLogTransformerImpl = new AdPlayLogTransformerImpl();
			adPlayLogTransformerImpl.setup(context);
		}

		bsEpgReduceStrategy = new BsEpgReduceStrategy();
		bsEpgReduceStrategy.setCurrCamusExecTime(currCamusExecTime);
		bsEpgReduceStrategy
				.setOrderDetailDumgBeetleTransformer(orderDetailDumgBeetleTransformer);

		ctypeReduceStrategy = new CtypeReduceStrategy();
		ctypeReduceStrategy.setCurrCamusExecTime(currCamusExecTime);
		ctypeReduceStrategy
				.setOrderDetailDumgBeetleTransformer(orderDetailDumgBeetleTransformer);

		ctypeReduceStrategy
				.setAdPlayLogTransformerImpl(adPlayLogTransformerImpl);
		ctypeReduceStrategy
				.setRunadPlayLogTransformer(isRunadPlayLogTransformer);
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		if (orderDetailDumgBeetleTransformer != null) {
			orderDetailDumgBeetleTransformer.cleanup(context);
		}

		if (adPlayLogTransformerImpl != null) {
			adPlayLogTransformerImpl.cleanup(context);
		}
	}

	@Override
	protected void reduce(Text sessionIdAndNatip,
			Iterable<AvroValue<SpecificRecordBase>> iterable, Context context)
			throws IOException, InterruptedException {

		String session = sessionIdAndNatip.toString();
		if (session.startsWith("bsepg-")) {
			bsEpgReduceStrategy.reduce(sessionIdAndNatip, iterable, context);
		} else {
			ctypeReduceStrategy.reduce(sessionIdAndNatip, iterable, context);
		}
	}

}
