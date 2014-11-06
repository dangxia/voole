package com.voole.dungbeetle.ad.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;

public class ReadAdException {
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://dev-test1:8082");
		Reader r = new SequenceFile.Reader(
				conf,
				SequenceFile.Reader
						.file(new Path(
								"/hive_order/history/2014-11-05-13-46-58/ad_transform_exception-r-00000.transform_exception")));
		Text text = new Text();
		while (r.next(NullWritable.get(), text)) {
			System.out.println(text.toString());
		}
		r.close();
	}
}
