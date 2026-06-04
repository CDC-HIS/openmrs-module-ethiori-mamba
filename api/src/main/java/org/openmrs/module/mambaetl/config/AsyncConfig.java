package org.openmrs.module.mambaetl.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AsyncConfig {
	
	private static final Log log = LogFactory.getLog(AsyncConfig.class);
	
	private static volatile Executor reportExecutor;
	
	private static volatile ScheduledExecutorService cleanupScheduler;
	
	private AsyncConfig() {
	}
	
	public static synchronized Executor getReportExecutor() {
        if (reportExecutor == null) {
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
                    "mamba-report executor is saturated; report job rejected. active=" + pool.getActiveCount()
                            + " queued=" + pool.getQueue().size()));
            executor.initialize();
            reportExecutor = executor;
        }
        return reportExecutor;
    }
	
	public static synchronized ScheduledExecutorService getCleanupScheduler() {
        if (cleanupScheduler == null) {
            cleanupScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "mamba-job-cleanup");
                t.setDaemon(true);
                return t;
            });
        }
        return cleanupScheduler;
    }
	
	public static void shutdown() {
		try {
			if (reportExecutor != null && reportExecutor instanceof ThreadPoolTaskExecutor) {
				((ThreadPoolTaskExecutor) reportExecutor).shutdown();
			}
		}
		catch (Exception e) {
			log.warn("Error shutting down report executor", e);
		}
		try {
			if (cleanupScheduler != null) {
				cleanupScheduler.shutdownNow();
			}
		}
		catch (Exception e) {
			log.warn("Error shutting down cleanup scheduler", e);
		}
	}
	
	private static int getIntGlobalProperty(String key, int defaultValue) {
		try {
			String val = org.openmrs.api.context.Context.getAdministrationService().getGlobalProperty(key);
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
