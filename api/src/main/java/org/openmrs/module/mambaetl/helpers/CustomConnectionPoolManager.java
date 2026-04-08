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
import org.openmrs.api.context.Context;

import java.sql.SQLException;
import java.util.Properties;

public class CustomConnectionPoolManager {
	
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
		
		dataSource.setInitialSize(2);
		dataSource.setMinIdle(2);
		dataSource.setMaxIdle(10);
		dataSource.setMaxTotal(20);
		
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
}
