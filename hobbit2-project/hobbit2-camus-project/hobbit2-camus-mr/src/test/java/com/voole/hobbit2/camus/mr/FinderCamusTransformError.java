/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.camus.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.voole.hobbit2.camus.mr.common.CamusKey;

/**
 * @author XuehuiHe
 * @date 2014年9月18日
 */

public class FinderCamusTransformError {
	@Test
	public void test() throws IOException {

		SequenceFile.Reader reader = new SequenceFile.Reader(
				new Configuration(),
				SequenceFile.Reader
						.file(new Path(
								"/camus/exec/history/2014-09-18-17-07-47/errors-m-00003.error")));
		CamusKey key = new CamusKey();
		Text msg = new Text();
		if (reader.next(key, msg)) {
			System.out.println(key);
			System.out.println(msg);
		}

		reader.close();

	}
}
