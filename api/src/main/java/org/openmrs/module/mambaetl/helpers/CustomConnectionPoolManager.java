/**
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 * <p>
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of Healthcare Disclaimer located at http://openmrs.org/license.
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
	
	private CustomConnectionPoolManager(String url, String userName, String password, int maxTotal, int maxIdle) {
		dataSource = new BasicDataSource();
		dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
		dataSource.setUsername(userName);
		dataSource.setPassword(password);
		dataSource.setUrl(url);
		
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
	 * Thread-safe double-checked locking singleton. Context reads (runtime properties, global
	 * properties) are performed BEFORE acquiring the class lock. This eliminates a potential
	 * lock-ordering deadlock: if Context.getAdministrationService() is called while holding the
	 * class lock, and the OpenMRS/Spring layer simultaneously holds a Spring ApplicationContext
	 * lock waiting to enter this class's synchronized block, both threads would wait forever. By
	 * reading config outside the lock, the synchronized block only performs fast, lock-free object
	 * construction and a volatile write.
	 */
	public static CustomConnectionPoolManager getInstance() {
		if (instance == null) {
			// Read all OpenMRS context values before acquiring the class lock.
			Properties properties = Context.getRuntimeProperties();
			String url = properties.getProperty("mambaetl.analysis.db.url");
			String userName = properties.getProperty("mambaetl.analysis.db.username");
			String password = properties.getProperty("mambaetl.analysis.db.password");
			
			String resolvedUrl = (url != null) ? url
			        : "jdbc:mysql://localhost:3306/analytics_db?autoReconnect=true&useSSL=false";
			String resolvedUser = userName != null ? userName : properties.getProperty("connection.username");
			String resolvedPass = password != null ? password : properties.getProperty("connection.password");
			
			int maxTotal = readIntGlobalProperty("mambaetl.analysis.db.pool.maxTotal", 15);
			int maxIdle = readIntGlobalProperty("mambaetl.analysis.db.pool.maxIdle", 6);
			
			synchronized (CustomConnectionPoolManager.class) {
				// Re-check under lock; another thread may have initialized while we were reading config.
				if (instance == null) {
					instance = new CustomConnectionPoolManager(resolvedUrl, resolvedUser, resolvedPass, maxTotal, maxIdle);
				}
			}
		}
		return instance;
	}
	
	public BasicDataSource getDataSource() {
		return dataSource;
	}
	
	/**
	 * Closes the pool if it has been initialized. Synchronized on the class lock so that concurrent
	 * calls from the module stopped() lifecycle and the Spring @PreDestroy hook are mutually
	 * exclusive — preventing a double-close race during module unloading.
	 */
	public static synchronized void shutdownIfInitialized() {
		if (instance != null) {
			instance.doClose();
			instance = null;
		}
	}
	
	/**
	 * Gracefully closes all connections in the pool.
	 * 
	 * @deprecated Use {@link #shutdownIfInitialized()} which is properly synchronized.
	 */
	@Deprecated
	public void closeDataSource() {
		shutdownIfInitialized();
	}
	
	private void doClose() {
		try {
			if (!dataSource.isClosed()) {
				dataSource.close();
			}
		}
		catch (SQLException ignored) {}
	}
	
	private static int readIntGlobalProperty(String key, int defaultValue) {
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
