/**
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 * <p>
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */
package org.openmrs.module.mambaetl.helpers;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.api.context.Context;

import java.sql.SQLException;
import java.util.Properties;

public class CustomConnectionPoolManager {
	
	private static final Log log = LogFactory.getLog(CustomConnectionPoolManager.class);
	
	private static volatile CustomConnectionPoolManager instance = null;
	
	private final BasicDataSource dataSource;
	
	private CustomConnectionPoolManager() {
		
		Properties properties = Context.getRuntimeProperties();
		
		String url = properties.getProperty("mambaetl.analysis.db.url");
		String userName = properties.getProperty("mambaetl.analysis.db.username");
		String password = properties.getProperty("mambaetl.analysis.db.password");
		
		String resolvedUrl = (url != null) ? url
		        : "jdbc:mysql://localhost:3306/analytics_db?autoReconnect=true&useSSL=false";
		
		dataSource = new BasicDataSource();
		dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
		dataSource.setUsername(userName != null ? userName : properties.getProperty("connection.username"));
		dataSource.setPassword(password != null ? password : properties.getProperty("connection.password"));
		dataSource.setUrl(resolvedUrl);
		
		// Pool sizes read from OpenMRS global properties at pool creation time.
		// Override via Admin > Global Properties:
		//   mambaetl.analysis.db.pool.maxTotal  (default 15, 16 GB target)
		//   mambaetl.analysis.db.pool.maxIdle   (default 6)
		int maxTotal = getIntGlobalProperty("mambaetl.analysis.db.pool.maxTotal", 15);
		int maxIdle = getIntGlobalProperty("mambaetl.analysis.db.pool.maxIdle", 6);
		
		dataSource.setInitialSize(2);
		dataSource.setMinIdle(2);
		dataSource.setMaxIdle(maxIdle);
		dataSource.setMaxTotal(maxTotal);
		
		dataSource.setMaxWait(java.time.Duration.ofSeconds(30));
		
		dataSource.setValidationQuery("SELECT 1");
		dataSource.setTestOnBorrow(true);
		dataSource.setTestWhileIdle(true);
		
		dataSource.setRemoveAbandonedOnMaintenance(true);
		dataSource.setRemoveAbandonedOnBorrow(true);
		dataSource.setRemoveAbandonedTimeout(java.time.Duration.ofSeconds(3600));
		dataSource.setLogAbandoned(true);
		
		dataSource.setDurationBetweenEvictionRuns(java.time.Duration.ofSeconds(60));
		dataSource.setMinEvictableIdle(java.time.Duration.ofMinutes(10));
	}
	
	/**
	 * Thread-safe double-checked locking singleton. Using {@code volatile} on the field prevents
	 * the broken-singleton problem caused by partial construction visibility.
	 */
	public static CustomConnectionPoolManager getInstance() {
		if (instance == null) {
			synchronized (CustomConnectionPoolManager.class) {
				if (instance == null) {
					instance = new CustomConnectionPoolManager();
				}
			}
		}
		return instance;
	}
	
	public BasicDataSource getDataSource() {
		return dataSource;
	}
	
	public static void shutdownIfInitialized() {
		if (instance != null) {
			instance.closeDataSource();
		}
	}
	
	/**
	 * Gracefully closes all connections in the pool. Should be called from the module's
	 * {@code stopped()} / {@code shutdown()} lifecycle hook to avoid FD leaks on module reload.
	 */
	public void closeDataSource() {
		try {
			if (!dataSource.isClosed()) {
				dataSource.close();
			}
		}
		catch (SQLException ignored) {}
		instance = null;
	}
	
	private static int getIntGlobalProperty(String key, int defaultValue) {
		try {
			String val = Context.getAdministrationService().getGlobalProperty(key);
			if (val != null && !val.trim().isEmpty()) {
				return Integer.parseInt(val.trim());
			}
		}
		catch (Exception e) {
			log.warn("Could not read global property " + key + ", using default " + defaultValue);
		}
		return defaultValue;
	}
}
