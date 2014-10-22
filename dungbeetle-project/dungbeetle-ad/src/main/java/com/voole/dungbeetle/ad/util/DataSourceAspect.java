package com.voole.dungbeetle.ad.util;

import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.stereotype.Service;

@Aspect
@Service
public class DataSourceAspect {
    
	@Before("execution (public * com.voole.dungbeetle.ad.dao..*.*(..)) && @annotation(type)")
	public void setDataSource(DataSourceType type) throws Throwable{
		DataSourceTypeHolder.setCustomerType(type.type());
	}
	
	
	@After("execution (public * com.voole.dungbeetle.ad.dao..*.*(..)) && @annotation(type)")
	public void resetDataSource(DataSourceType type) throws Throwable{
		DataSourceTypeHolder.clearCustomerType();
	}
	
	
}
