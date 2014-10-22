package com.voole.dungbeetle.ad.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;

/**
 * 自定义PropertyPlaceholderConfigurer返回properties内容
 * 
 * @author  2014-05-28
 * 
 */
public class GlobalProperties extends PropertyPlaceholderConfigurer{

	private static Map<String, Object> ctxPropertiesMap;

	@Override
	protected void processProperties(ConfigurableListableBeanFactory beanFactoryToProcess,Properties props) throws BeansException {
		
		super.processProperties(beanFactoryToProcess, props);
		ctxPropertiesMap = new HashMap<String, Object>();
		for (Object key : props.keySet()) {
			String keyStr = key.toString();
			String value = props.getProperty(keyStr);
			ctxPropertiesMap.put(keyStr, value);
		}
	}

	public static Object getContextProperty(String name) {
		return ctxPropertiesMap.get(name);
	}
	
	/**
	 * 获取属性值
	 * @param key
	 * @return
	 */
	public static String getProperties(String key){ 
		Object value = ctxPropertiesMap.get(key);
		return value != null ? String.valueOf(value) : "";
	}
	
	 
	/**
	 *获取属性值，返回整形
	 * @param key
	 * @return
	 */
	public static Integer getInteger(String key){
		Object value = ctxPropertiesMap.get(key);
		return value != null ? Integer.valueOf(value.toString()) : 0;
	}

}
