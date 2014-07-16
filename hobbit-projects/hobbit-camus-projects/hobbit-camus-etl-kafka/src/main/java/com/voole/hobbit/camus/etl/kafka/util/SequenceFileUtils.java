/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit.camus.etl.kafka.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;

/**
 * @author XuehuiHe
 * @date 2014年7月16日
 */
public class SequenceFileUtils {
	public static Writer createWriter(FileSystem fs, Configuration conf,
			Path name, Class<?> keyClass, Class<?> valClass) throws IOException {
		return SequenceFile.createWriter(conf, Writer.file(name),
				Writer.keyClass(keyClass), Writer.valueClass(valClass));
	}

	public static Reader createReader(FileSystem fs, Path file, Configuration conf)
			throws IOException {
		return new SequenceFile.Reader(conf, SequenceFile.Reader.file(file));
	}

}
