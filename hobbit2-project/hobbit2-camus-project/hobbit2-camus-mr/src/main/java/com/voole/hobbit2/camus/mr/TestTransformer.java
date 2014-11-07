/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.mr;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import com.voole.hobbit2.camus.OrderTopicsUtils;
import com.voole.hobbit2.camus.api.transform.AvroCtypeKafkaTransformer;
import com.voole.hobbit2.camus.api.transform.TransformException;
import com.voole.hobbit2.camus.mr.common.CamusKey;

/**
 * @author XuehuiHe
 * @date 2014年9月18日
 */
public class TestTransformer {
	public static final Configuration conf = new Configuration();
	public static final Map<String, AvroCtypeKafkaTransformer> topicToTransformer = new HashMap<String, AvroCtypeKafkaTransformer>();
	static {
		conf.addResource("core-site2.xml");
		conf.setBoolean("order.detail.transformer.is.auto.refresh.cache", true);
	}

	public static void main(String[] args) throws TransformException,
			IOException {
		FileWriter writer = new FileWriter("/tmp/errorRecord.txt");

		test("/camus/exec/history/2014-11-07-15-01-06/errors-m-00004.error",
				writer);
		test("/camus/exec/history/2014-11-07-15-01-06/errors-m-00005.error",
				writer);
		test("/camus/exec/history/2014-11-07-15-01-06/errors-m-00006.error",
				writer);
		test("/camus/exec/history/2014-11-07-15-01-06/errors-m-00007.error",
				writer);
		test("/camus/exec/history/2014-11-07-15-01-06/errors-m-00016.error",
				writer);
		test("/camus/exec/history/2014-11-07-15-01-06/errors-m-00017.error",
				writer);
		test("/camus/exec/history/2014-11-07-15-01-06/errors-m-00018.error",
				writer);
		test("/camus/exec/history/2014-11-07-15-01-06/errors-m-00019.error",
				writer);
		writer.close();
		// String msg =
		// "3249065509265744850	1411021973	629213	1411030260	0	29206	0	0	1929807	0	10	4100331066	1	8228	14554	16421	29	526154	550135	13543	10	4100331066	1	8204	14585	32842	59	488530	616334	13350	7	4117108282	1	7731	15594	3217	28	478263	111022	12864	11	4117108282	1	7403	15966	16421	42	470156	388129	22865	8	4133885498	1	6924	17164	32842	73	432409	419363	16761	9	4133885498	1	6887	17402	32842	72	369068	467089	16048	6	934937821	1	2640	44542	0	0	179704	187398	44274	21	884606173	1	2589	47759	0	0	171098	159917	51952	25	934937821	1	2697	44097	0	0	170813	183933	44042	22	884606173	1	2333	54437	0	0	105187	106559	80932	24	0";
		// AvroCtypeKafkaTransformer transformer = new
		// AvroCtypeKafkaTransformer(
		// OrderPlayAliveReqV3.getClassSchema());
		// transformer.transform(msg.getBytes());
	}

	public static void test(String file, FileWriter writer) throws IOException,
			TransformException {
		Path path = new Path(file);
		SequenceFile.Reader reader = new SequenceFile.Reader(conf,
				SequenceFile.Reader.file(path));
		CamusKey key = new CamusKey();
		Text msg = new Text();

		while (reader.next(key, msg)) {
			System.out.println(key.getTopic());
			System.out.println(msg);
			try {
				AvroCtypeKafkaTransformer transformer = getTransform(key
						.getTopic());
				System.out.println(transformer.transform(msg.getBytes()));
			} catch (Exception e) {
				if (e.getMessage().indexOf("repeatTimes") != -1) {
					writer.append("topic:" + key.getTopic() + "\n");
					writer.append("msg:" + msg + "\n");
				}
			}

		}
		reader.close();
	}

	public static AvroCtypeKafkaTransformer getTransform(String topic)
			throws TransformException {
		if (!topicToTransformer.containsKey(topic)) {
			topicToTransformer.put(topic, new AvroCtypeKafkaTransformer(
					OrderTopicsUtils.topicBiSchema.get(topic)));
		}
		return topicToTransformer.get(topic);
	}
}
