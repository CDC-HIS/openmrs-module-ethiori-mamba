package org.openmrs.module.mambaetl.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.module.mambaetl.helpers.CustomConnectionPoolManager;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;

@Component
public class ConnectionPoolShutdownHook {
	
	private static final Log log = LogFactory.getLog(ConnectionPoolShutdownHook.class);
	
	@PreDestroy
	public void shutdown() {
		log.info("Shutting down analytics connection pool");
		try {
			CustomConnectionPoolManager.shutdownIfInitialized();
		}
		catch (Exception e) {
			log.warn("Failed to close analytics connection pool during shutdown", e);
		}
	}
}
