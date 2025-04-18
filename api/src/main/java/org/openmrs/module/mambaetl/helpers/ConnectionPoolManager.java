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

import java.util.Properties;

public class ConnectionPoolManager {
	
	private static ConnectionPoolManager instance = null;
	
	private static final BasicDataSource dataSource = new BasicDataSource();
	
	private ConnectionPoolManager() {
		
		Properties properties = Context.getRuntimeProperties();
		
		String url = properties.getProperty("mambaetl.analysis.db.url");
		String userName = properties.getProperty("mambaetl.analysis.db.username");
		String password = properties.getProperty("mambaetl.analysis.db.password");
		//	String url = "jdbc:mysql://localhost:3306/analysis_db?autoReconnect=true&useSSL=false";
		
		dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
		dataSource.setUsername(userName != null ? userName : properties.getProperty("connection.username"));
		dataSource.setPassword((password != null ? password : properties.getProperty("connection.password")));
		dataSource.setUrl((url != null ? url : properties
		        .getProperty("jdbc:mysql://localhost:3306/analytics_db?autoReconnect=true&useSSL=false")));
		
		dataSource.setInitialSize(4);
		dataSource.setMaxTotal(20);
	}
	
	public static ConnectionPoolManager getInstance() {
		if (instance == null) {
			instance = new ConnectionPoolManager();
		}
		return instance;
	}
	
	public BasicDataSource getDataSource() {
		return dataSource;
	}
}
