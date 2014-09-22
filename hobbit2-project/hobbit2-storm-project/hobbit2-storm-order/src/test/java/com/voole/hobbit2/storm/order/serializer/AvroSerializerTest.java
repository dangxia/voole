/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order.serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.specific.SpecificRecordBase;
import org.junit.Assert;
import org.junit.Test;

import com.voole.hobbit2.camus.order.OrderPlayEndReqV2;

/**
 * @author XuehuiHe
 * @date 2014年9月22日
 */
public class AvroSerializerTest {
	@Test
	public void test1() throws IOException {
		AvroSerializer<SpecificRecordBase> serializer = new AvroSerializer<SpecificRecordBase>();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		OrderPlayEndReqV2 end = new OrderPlayEndReqV2();
		end.setEndTick(111l);
		serializer.write(null, new com.esotericsoftware.kryo.io.Output(out),
				end);
		out.close();

		ByteArrayInputStream input = new ByteArrayInputStream(out.toByteArray());
		Object _new = serializer.read(null,
				new com.esotericsoftware.kryo.io.Input(input), null);

		Assert.assertFalse(end == _new);
		Assert.assertTrue(_new.getClass() == OrderPlayEndReqV2.class);
		Assert.assertEquals(end, _new);

	}
}
