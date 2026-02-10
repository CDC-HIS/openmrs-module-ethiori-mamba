package org.openmrs.module.mambaetl.api.db;

import org.apache.commons.dbcp2.BasicDataSource;
import org.openmrs.module.mambacore.util.MambaETLProperties;

public class ETLConnectionPoolManager {
	
	private static final BasicDataSource dataSource = new BasicDataSource();
	
	private static ETLConnectionPoolManager instance = null;
	
	private final MambaETLProperties props = MambaETLProperties.getInstance();
	
	private ETLConnectionPoolManager() {
		
		dataSource.setDriverClassName(props.getOpenmrsDbDriver());
		dataSource.setUsername(props.getMambaETLuser());
		dataSource.setPassword(props.getMambaETLuserPassword());
		dataSource.setUrl(props.getOpenmrsDbConnectionUrl());
		
		dataSource.setInitialSize(props.getConnectionInitialSize());
		dataSource.setMaxTotal(props.getConnectionMaxTotal());
	}
	
	public static ETLConnectionPoolManager getInstance() {
		if (instance == null) {
			instance = new ETLConnectionPoolManager();
		}
		return instance;
	}
	
	public BasicDataSource getDefaultDataSource() {
		return dataSource;
	}
	
	public BasicDataSource getEtlDataSource() {
		
		String etlDatabase = props.getEtlDatababase();
		String modifiedUrl = getModifiedUrl(dataSource.getUrl(), etlDatabase);
		
		BasicDataSource etlDataSource = new BasicDataSource();
		etlDataSource.setDefaultSchema(etlDatabase);
		etlDataSource.setUrl(modifiedUrl);
		etlDataSource.setDriverClassName(dataSource.getDriverClassName());
		etlDataSource.setUsername(props.getMambaETLuser());
		etlDataSource.setPassword(props.getMambaETLuserPassword());
		etlDataSource.setInitialSize(dataSource.getInitialSize());
		etlDataSource.setMaxTotal(dataSource.getMaxTotal());
		
		return etlDataSource;
	}
	
	private String getModifiedUrl(String originalUrl, String newDatabase) {
		return originalUrl.replaceAll("(/)[^/?]+(?=\\?|$)", "$1" + newDatabase);
	}
}
