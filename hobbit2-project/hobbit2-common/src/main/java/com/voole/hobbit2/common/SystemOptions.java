/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.voole.hobbit2.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

/**
 * @author XuehuiHe
 * @date 2014年7月11日
 */
public class SystemOptions {
	@SuppressWarnings("static-access")
	public static Properties process(String[] args) throws IOException,
			ParseException {
		Options options = new Options();

		options.addOption("p", true, "properties filename from the classpath");
		options.addOption("P", true, "external properties filename");

		options.addOption(OptionBuilder.withArgName("property=value")
				.hasArgs(2).withValueSeparator()
				.withDescription("use value for given property").create("D"));
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);

		Properties props = new Properties();
		if (cmd.hasOption('p'))
			props.load(SystemOptions.class.getClassLoader()
					.getResourceAsStream(cmd.getOptionValue('p')));

		if (cmd.hasOption('P')) {
			File file = new File(cmd.getOptionValue('P'));
			FileInputStream fStream = new FileInputStream(file);
			props.load(fStream);
		}

		props.putAll(cmd.getOptionProperties("D"));

		return props;
	}
}
