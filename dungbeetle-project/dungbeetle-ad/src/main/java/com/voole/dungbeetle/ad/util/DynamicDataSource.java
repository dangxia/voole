package com.voole.dungbeetle.ad.util;

import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

public class DynamicDataSource extends AbstractRoutingDataSource {

	@Override
	protected Object determineCurrentLookupKey() {
		String type = DataSourceTypeHolder.getCustomerType();
		return type;
	}


}
