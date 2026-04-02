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
	
	// Volatile ensures visibility of the fully-constructed instance across threads.
	private static volatile CustomConnectionPoolManager instance = null;
	
	private final BasicDataSource dataSource;
	
	private CustomConnectionPoolManager() {
		
		Properties properties = Context.getRuntimeProperties();
		
		String url = properties.getProperty("mambaetl.analysis.db.url");
		String userName = properties.getProperty("mambaetl.analysis.db.username");
		String password = properties.getProperty("mambaetl.analysis.db.password");
		
		// FIX: the original code passed a JDBC URL string as a property key which always
		// returned null. Use the hardcoded analytics_db URL as the intended fallback.
		String resolvedUrl = (url != null) ? url
		        : "jdbc:mysql://localhost:3306/analytics_db?autoReconnect=true&useSSL=false";
		
		dataSource = new BasicDataSource();
		dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
		dataSource.setUsername(userName != null ? userName : properties.getProperty("connection.username"));
		dataSource.setPassword(password != null ? password : properties.getProperty("connection.password"));
		dataSource.setUrl(resolvedUrl);
		
		// --- Pool sizing ---
		dataSource.setInitialSize(2);
		dataSource.setMinIdle(2);
		dataSource.setMaxIdle(10);
		dataSource.setMaxTotal(20);
		
		// --- Connection wait / timeout ---
		// Wait at most 30 s for a connection before throwing an error instead of blocking forever.
		dataSource.setMaxWait(java.time.Duration.ofSeconds(30));
		
		// --- Connection validation ---
		// Evict and replace stale/broken connections before handing them to callers.
		dataSource.setValidationQuery("SELECT 1");
		dataSource.setTestOnBorrow(true);
		dataSource.setTestWhileIdle(true);
		
		// --- Abandoned connection recovery ---
		// If a connection is checked out for more than 5 minutes it is considered abandoned
		// (e.g. due to an unhandled exception) and will be reclaimed by the pool.
		dataSource.setRemoveAbandonedOnMaintenance(true);
		dataSource.setRemoveAbandonedOnBorrow(true);
		dataSource.setRemoveAbandonedTimeout(java.time.Duration.ofSeconds(300));
		dataSource.setLogAbandoned(true); // log the stack trace of the leak site
		
		// --- Idle eviction ---
		// Run the eviction thread every 60 s to close connections idle for over 10 minutes.
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
		catch (SQLException e) {
			// Nothing useful we can do here; log if a logger is available.
		}
		// Allow a fresh pool to be created if the module is restarted in the same JVM.
		instance = null;
	}
}
