package org.openmrs.module.mambaetl.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.api.context.Context;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
@EnableAsync
@EnableScheduling
public class AsyncConfig {

	private static final Log log = LogFactory.getLog(AsyncConfig.class);

	/**
	 * Thread pool sized for a 16 GB machine by default. Override via OpenMRS global properties:
	 *   mambaetl.report.executor.corePoolSize  (default 4)
	 *   mambaetl.report.executor.maxPoolSize   (default 8)
	 *   mambaetl.report.executor.queueCapacity (default 50)
	 *
	 * Suggested values:
	 *   8 GB desktop : 2 / 4 / 25
	 *   16 GB server : 4 / 8 / 50  (default)
	 *   32 GB server : 8 / 16 / 100
	 */
	@Bean(name = "mambaReportExecutor")
	public Executor mambaReportExecutor() {
		int core = getIntGlobalProperty("mambaetl.report.executor.corePoolSize", 4);
		int max = getIntGlobalProperty("mambaetl.report.executor.maxPoolSize", 8);
		int queue = getIntGlobalProperty("mambaetl.report.executor.queueCapacity", 50);

		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(core);
		executor.setMaxPoolSize(max);
		executor.setQueueCapacity(queue);
		executor.setThreadNamePrefix("mamba-report-");
		executor.setWaitForTasksToCompleteOnShutdown(true);
		executor.setAwaitTerminationSeconds(60);
		executor.setRejectedExecutionHandler((r, pool) -> log.warn(
		    "mamba-report executor is saturated; report job rejected. active=" + pool.getActiveCount() + " queued="
		            + pool.getQueue().size()));
		executor.initialize();
		return executor;
	}

	private int getIntGlobalProperty(String key, int defaultValue) {
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
