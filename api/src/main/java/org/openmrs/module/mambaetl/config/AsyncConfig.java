package org.openmrs.module.mambaetl.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
	
	@Bean(name = "mambaReportExecutor")
	public Executor mambaReportExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(4);
		executor.setMaxPoolSize(8);
		executor.setQueueCapacity(50);
		executor.setThreadNamePrefix("mamba-report-");
		executor.setWaitForTasksToCompleteOnShutdown(true);
		executor.setAwaitTerminationSeconds(60);
		executor.setRejectedExecutionHandler((r, pool) -> log.warn(
		    "mamba-report executor is saturated; report job rejected. active=" + pool.getActiveCount() + " queued="
		            + pool.getQueue().size()));
		executor.initialize();
		return executor;
	}
}
