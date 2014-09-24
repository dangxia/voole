/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.storm.order;

import java.io.IOException;

import org.apache.avro.file.FileReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.fs.Path;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.voole.hobbit2.storm.order.kryodecorator.TestKryoDecorator;

/**
 * @author XuehuiHe
 * @date 2014年9月24日
 */
public class TestTest {
	public static void main(String[] args) throws IOException {

		Kryo k = new Kryo();
		TestKryoDecorator t = new TestKryoDecorator();
		t.decorate(k);
		SpecificRecordBase recordBase = null;
		Output out = new com.esotericsoftware.kryo.io.Output(System.out);
		String[] paths = new String[] {
				"/hive_order/history/2014-09-22-15-22-02/noend_t_playbgn_v2-r-00000.avro",
				"/hive_order/history/2014-09-22-15-22-02/noend_t_playalive_v3-r-00004.avro" };
		for (String string : paths) {
			Path path = new Path(string);
			FileReader<SpecificRecordBase> reader = StormOrderHDFSUtils
					.getNoendReader(path);

			while (reader.hasNext()) {
				recordBase = reader.next(recordBase);
				k.writeObject(out, recordBase);
			}
			reader.close();
		}

		out.flush();
		out.close();

	}
}
