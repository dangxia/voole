package com.voole.hobbit2.common;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;

import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;

public class Hobbit2Configuration {
	public static final String PROP_FILE_LIST = "prop.file.list";

	private static Logger logger = LoggerFactory
			.getLogger(Hobbit2Configuration.class);

	public static CompositeConfiguration initConfig()
			throws ConfigurationException, FileNotFoundException {
		return initConfig(new Properties());
	}

	public static CompositeConfiguration initConfig(String[] args)
			throws ConfigurationException, IOException, ParseException {

		return initConfig(SystemOptions.process(args));
	}

	public static CompositeConfiguration initConfig(Properties props)
			throws ConfigurationException, FileNotFoundException {
		String[] propFileNames = null;
		if (props != null && props.containsKey(PROP_FILE_LIST)) {
			String listStr = props.getProperty(PROP_FILE_LIST);
			propFileNames = Splitter.on(',').splitToList(listStr)
					.toArray(new String[] {});
		} else {
			PropertiesConfiguration item = new PropertiesConfiguration();
			Optional<URL> url = findPropFileUrl("prop.file.site.properties");
			if (url.isPresent()) {
				logger.info("load props from file : prop.file.site.properties");
				item.load(url.get());
			} else {
				url = findPropFileUrl("prop.file.default.properties");
				if (url.isPresent()) {
					logger.info("load props from file : prop.file.default.properties");
					item.load(url.get());
				} else {
					throw new FileNotFoundException(
							"file:prop.file.default.properties not found");
				}
			}
			propFileNames = item.getStringArray(PROP_FILE_LIST);
		}

		CompositeConfiguration configuration = new CompositeConfiguration();
		for (String name : propFileNames) {
			configuration.addConfiguration(getProps(name));
		}
		if (props != null) {
			for (Object key : props.keySet()) {
				String _k = (String) key;
				configuration.setProperty(_k, props.getProperty(_k));
			}
		}
		return configuration;
	}

	public static PropertiesConfiguration getProps(String name)
			throws ConfigurationException {
		PropertiesConfiguration item = new PropertiesConfiguration();
		item.setDelimiterParsingDisabled(true);
		Optional<URL> url = findPropFileUrl(name + ".site.properties");
		if (url.isPresent()) {
			logger.info("load props from file : " + name + ".site.properties");
			item.load(url.get());
		}
		url = findPropFileUrl(name + ".default.properties");
		if (url.isPresent()) {
			logger.info("load props from file : " + name
					+ ".default.properties");
			item.load(url.get());
		} else {
			logger.warn("file:" + name + ".default.properties" + " not found");
		}

		return item;
	}

	public static Optional<URL> findPropFileUrl(String filename) {
		URL url = ConfigurationUtils.locate(null, filename);
		if (url == null) {
			return Optional.absent();
		}
		return Optional.of(url);
	}

}
