package org.openmrs.module.mambaetl.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.module.mambaetl.web.resource.ReportDataResponse;
import org.openmrs.module.mambaetl.web.resource.ReportJob;
import org.openmrs.module.mambaetl.web.resource.ReportJobStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ReportJobService implements ApplicationContextAware {

	private static final Log log = LogFactory.getLog(ReportJobService.class);

	private final ConcurrentHashMap<String, ReportJob> jobs = new ConcurrentHashMap<>();

	@Autowired
	private DynamicReportExecutorService reportExecutorService;

	private ApplicationContext applicationContext;

	@Override
	public void setApplicationContext(ApplicationContext ctx) {
		this.applicationContext = ctx;
	}

	public ReportJob submitJob(String procedureName, Map<String, String> params, int offset, int limit) {
		String jobId = UUID.randomUUID().toString();
		ReportJob job = new ReportJob(jobId, procedureName);
		jobs.put(jobId, job);
		try {
			CompletableFuture<?> future = applicationContext.getBean(ReportJobService.class)
			        .executeJobAsync(job, params, offset, limit);
			job.setFuture(future);
		}
		catch (Exception e) {
			log.error("Failed to queue report job " + jobId, e);
			synchronized (job) {
				job.setError("Failed to queue job: " + e.getMessage());
				job.setCompletedAt(Instant.now());
				job.setStatus(ReportJobStatus.ERROR);
			}
		}
		return job;
	}

	@Async("mambaReportExecutor")
	public CompletableFuture<Void> executeJobAsync(ReportJob job, Map<String, String> params, int offset, int limit) {
		synchronized (job) {
			job.setStatus(ReportJobStatus.RUNNING);
		}
		try {
			DynamicReportExecutorService.ReportExecutionResult result = reportExecutorService.executeReport(
			    job.getProcedureName(), params, offset, limit);
			synchronized (job) {
				job.setResult(new ReportDataResponse(job.getProcedureName(), result.getData()));
				job.setCompletedAt(Instant.now());
				job.setStatus(ReportJobStatus.COMPLETE);
			}
		}
		catch (Exception e) {
			log.error("Async report job failed for procedure " + job.getProcedureName(), e);
			synchronized (job) {
				job.setError("Stored procedure execution failed: " + e.getMessage());
				job.setCompletedAt(Instant.now());
				job.setStatus(ReportJobStatus.ERROR);
			}
		}
		return CompletableFuture.completedFuture(null);
	}

	public ReportJob getJob(String jobId) {
		return jobs.get(jobId);
	}

	public boolean cancelJob(String jobId) {
		ReportJob job = jobs.get(jobId);
		if (job == null) {
			return false;
		}
		synchronized (job) {
			ReportJobStatus status = job.getStatus();
			if (status == ReportJobStatus.COMPLETE || status == ReportJobStatus.ERROR) {
				return false;
			}
			if (job.getFuture() != null) {
				job.getFuture().cancel(true);
			}
			job.setError("Job cancelled by client");
			job.setCompletedAt(Instant.now());
			job.setStatus(ReportJobStatus.ERROR);
		}
		return true;
	}

	@Scheduled(fixedDelay = 600_000)
	public void cleanupExpiredJobs() {
		Instant cutoff = Instant.now().minusSeconds(7200);
		jobs.entrySet().removeIf(entry -> {
			ReportJob job = entry.getValue();
			ReportJobStatus status = job.getStatus();
			Instant completedAt = job.getCompletedAtInstant();
			return (status == ReportJobStatus.COMPLETE || status == ReportJobStatus.ERROR)
			        && completedAt != null && completedAt.isBefore(cutoff);
		});
	}
}
