/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.meta.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.mapreduce.Reducer;

import com.voole.hobbit2.camus.meta.common.CamusMapperTimeKeyAvro;

/**
 * @author XuehuiHe
 * @date 2014年8月27日
 */
public class CamusReducer
		extends
		Reducer<AvroKey<CamusMapperTimeKeyAvro>, AvroValue<SpecificRecordBase>, AvroKey<CamusMapperTimeKeyAvro>, AvroValue<SpecificRecordBase>> {
	@Override
	protected void reduce(AvroKey<CamusMapperTimeKeyAvro> key,
			Iterable<AvroValue<SpecificRecordBase>> iterable, Context context)
			throws IOException, InterruptedException {
		Iterator<AvroValue<SpecificRecordBase>> iterator = iterable.iterator();
		while (iterator.hasNext()) {
			context.write(key, iterator.next());
		}
	}
}
