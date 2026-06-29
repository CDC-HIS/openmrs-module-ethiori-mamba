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

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ReportJobService implements ApplicationContextAware {

	private static final Log log = LogFactory.getLog(ReportJobService.class);

	private final ConcurrentHashMap<String, ReportJob> jobs = new ConcurrentHashMap<>();

	private final ConcurrentHashMap<String, CallableStatement> activeStatements = new ConcurrentHashMap<>();

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
		job.setMessage("Queued, waiting for executor thread");
		jobs.put(jobId, job);
		// Resolve OpenMRS global-property config here, in the web-request thread that has a valid
		// OpenMRS session. The async executor thread has no OpenMRS session context, so calling
		// Context.getAdministrationService() there can open a Hibernate session without a proper
		// cleanup path, potentially exhausting the main connection pool under load.
		int queryTimeout = reportExecutorService.getQueryTimeoutSeconds();
		int maxRows = reportExecutorService.getMaxRows();
		try {
			CompletableFuture<?> future = applicationContext.getBean(ReportJobService.class)
			        .executeJobAsync(job, params, offset, limit, queryTimeout, maxRows);
			job.setFuture(future);
		}
		catch (Exception e) {
			log.error("Failed to queue report job " + jobId, e);
			synchronized (job) {
				job.setError("Failed to queue job: " + e.getMessage());
				job.setCompletedAt(Instant.now());
				job.setStatus(ReportJobStatus.ERROR);
				job.setMessage("Failed to queue job");
			}
		}
		return job;
	}

	@Async("mambaReportExecutor")
	public CompletableFuture<Void> executeJobAsync(ReportJob job, Map<String, String> params, int offset, int limit,
	        int queryTimeout, int maxRows) {
		synchronized (job) {
			job.setStatus(ReportJobStatus.RUNNING);
			job.setMessage("Executing stored procedure: " + job.getProcedureName());
		}
		try {
			DynamicReportExecutorService.ReportExecutionResult result = reportExecutorService.executeReport(
			    job.getProcedureName(), params, offset, limit,
			    (completed, total) -> {
				    synchronized (job) {
					    job.setTotalSteps(total);
					    job.setCompletedSteps(completed);
				    }
			    },
			    stmt -> activeStatements.put(job.getJobId(), stmt), queryTimeout, maxRows);
			synchronized (job) {
				if (job.getStatus() != ReportJobStatus.ERROR) {
					job.setResult(new ReportDataResponse(job.getProcedureName(), result.getData()));
					job.setCompletedAt(Instant.now());
					job.setStatus(ReportJobStatus.COMPLETE);
					job.setMessage("Completed successfully");
				}
			}
		}
		catch (Exception e) {
			log.error("Async report job failed for procedure " + job.getProcedureName(), e);
			synchronized (job) {
				if (job.getStatus() != ReportJobStatus.ERROR) {
					job.setError("Stored procedure execution failed: " + e.getMessage());
					job.setCompletedAt(Instant.now());
					job.setStatus(ReportJobStatus.ERROR);
					job.setMessage("Job failed");
				}
			}
		}
		finally {
			activeStatements.remove(job.getJobId());
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
			job.setMessage("Cancelled by client");
		}
		// Send KILL QUERY to MySQL — must happen outside the synchronized block so it
		// doesn't block the executor thread trying to clear the statement via the registrar.
		CallableStatement stmt = activeStatements.remove(jobId);
		if (stmt != null) {
			try {
				stmt.cancel();
			}
			catch (SQLException e) {
				log.warn("Failed to cancel active DB statement for job " + jobId + ": " + e.getMessage());
			}
		}
		return true;
	}

	@Scheduled(fixedDelay = 600_000)
	public void cleanupExpiredJobs() {
		long ttlSeconds = getJobTtlSeconds();
		Instant cutoff = Instant.now().minusSeconds(ttlSeconds);
		jobs.entrySet().removeIf(entry -> {
			ReportJob job = entry.getValue();
			ReportJobStatus status = job.getStatus();
			Instant completedAt = job.getCompletedAtInstant();
			return (status == ReportJobStatus.COMPLETE || status == ReportJobStatus.ERROR)
			        && completedAt != null && completedAt.isBefore(cutoff);
		});
	}

	private long getJobTtlSeconds() {
		return 1800L;
	}
}
