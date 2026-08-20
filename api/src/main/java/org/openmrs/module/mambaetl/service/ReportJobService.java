package org.openmrs.module.mambaetl.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.NonNull;
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
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ReportJobService implements ApplicationContextAware {

	private static final Log log = LogFactory.getLog(ReportJobService.class);

	// Underlying stored-procedure runs, keyed by executionId.
	private final ConcurrentHashMap<String, ReportJobExecution> executions = new ConcurrentHashMap<>();

	// Every client-facing handle issued by submitJob(), keyed by handleId -> the executionId it
	// is attached to. Several handles point at the same execution when requests get deduped.
	private final ConcurrentHashMap<String, String> handles = new ConcurrentHashMap<>();

	private final ConcurrentHashMap<String, CallableStatement> activeStatements = new ConcurrentHashMap<>();

	private final ConcurrentHashMap<String, String> inFlightKeys = new ConcurrentHashMap<>();

	@Autowired
	private DynamicReportExecutorService reportExecutorService;

	private ApplicationContext applicationContext;

	@Override
	public void setApplicationContext(@NonNull ApplicationContext ctx) {
		this.applicationContext = ctx;
	}

	public ReportJob submitJob(String procedureName, Map<String, String> params, int offset, int limit) {
		String dedupeKey = buildDedupeKey(procedureName, params, offset, limit);
		String handleId = UUID.randomUUID().toString();

		String existingExecutionId = inFlightKeys.get(dedupeKey);
		ReportJobExecution existingExecution = existingExecutionId != null ? executions.get(existingExecutionId) : null;
		if (existingExecution != null && isActive(existingExecution.getStatus())) {
			attachHandle(handleId, existingExecution);
			log.info(
			    "Reusing in-flight report execution " + existingExecutionId + " for duplicate request: " + dedupeKey);
			return toDto(handleId, existingExecution);
		}

		String executionId = UUID.randomUUID().toString();
		ReportJobExecution execution = new ReportJobExecution(executionId, procedureName, dedupeKey);
		executions.put(executionId, execution);

		String racedExecutionId = inFlightKeys.putIfAbsent(dedupeKey, executionId);
		if (racedExecutionId != null) {
			ReportJobExecution racedExecution = executions.get(racedExecutionId);
			if (racedExecution != null && isActive(racedExecution.getStatus())) {
				// Lost the race to start this execution — discard our stub and join the winner.
				executions.remove(executionId);
				attachHandle(handleId, racedExecution);
				return toDto(handleId, racedExecution);
			}
			// Previous holder already finished — take over as the new in-flight owner.
			inFlightKeys.put(dedupeKey, executionId);
		}

		attachHandle(handleId, execution);

		int queryTimeout = reportExecutorService.getQueryTimeoutSeconds();
		int maxRows = reportExecutorService.getMaxRows();
		try {
			CompletableFuture<?> future = applicationContext.getBean(ReportJobService.class)
			        .executeJobAsync(execution, params, offset, limit, queryTimeout, maxRows);
			execution.setFuture(future);
		}
		catch (Exception e) {
			log.error("Failed to queue report job " + executionId, e);
			synchronized (execution.getLock()) {
				execution.setError("Failed to queue job: " + e.getMessage());
				execution.setCompletedAt(Instant.now());
				execution.setStatus(ReportJobStatus.ERROR);
				execution.setMessage("Failed to queue job");
			}
			inFlightKeys.remove(dedupeKey, executionId);
		}
		return toDto(handleId, execution);
	}

	private void attachHandle(String handleId, ReportJobExecution execution) {
		handles.put(handleId, execution.getExecutionId());
		execution.getSubscriberHandleIds().add(handleId);
	}

	private boolean isActive(ReportJobStatus status) {
		return status == ReportJobStatus.PENDING || status == ReportJobStatus.RUNNING;
	}

	private ReportJob toDto(String handleId, ReportJobExecution execution) {
		return new ReportJob(handleId, execution.getStatus(), execution.getProcedureName(), execution.getSubmittedAt(),
		        execution.getCompletedAt(), execution.getResult(), execution.getError(), execution.getMessage(),
		        execution.getTotalSteps(), execution.getCompletedSteps());
	}

	private String buildDedupeKey(String procedureName, Map<String, String> params, int offset, int limit) {
		StringBuilder key = new StringBuilder(procedureName).append('|').append(offset).append('|').append(limit);
		for (Map.Entry<String, String> entry : new TreeMap<>(params).entrySet()) {
			key.append('|').append(entry.getKey()).append('=').append(entry.getValue());
		}
		return key.toString();
	}

	@Async("mambaReportExecutor")
	public CompletableFuture<Void> executeJobAsync(ReportJobExecution execution, Map<String, String> params, int offset,
	        int limit, int queryTimeout, int maxRows) {
		synchronized (execution.getLock()) {
			execution.setStatus(ReportJobStatus.RUNNING);
			execution.setMessage("Executing stored procedure: " + execution.getProcedureName());
		}
		try {
			DynamicReportExecutorService.ReportExecutionResult result = reportExecutorService.executeReport(
			    execution.getProcedureName(), params, offset, limit,
			    (completed, total) -> {
				    synchronized (execution.getLock()) {
					    execution.setTotalSteps(total);
					    execution.setCompletedSteps(completed);
				    }
			    },
			    stmt -> {
				    if (stmt != null) {
					    activeStatements.put(execution.getExecutionId(), stmt);
				    } else {
					    activeStatements.remove(execution.getExecutionId());
				    }
			    }, queryTimeout, maxRows);
			synchronized (execution.getLock()) {
				if (execution.getStatus() != ReportJobStatus.ERROR) {
					execution.setResult(new ReportDataResponse(execution.getProcedureName(), result.getData()));
					execution.setCompletedAt(Instant.now());
					execution.setStatus(ReportJobStatus.COMPLETE);
					execution.setMessage("Completed successfully");
				}
			}
		}
		catch (Exception e) {
			log.error("Async report job failed for procedure " + execution.getProcedureName(), e);
			synchronized (execution.getLock()) {
				if (execution.getStatus() != ReportJobStatus.ERROR) {
					execution.setError("Stored procedure execution failed: " + e.getMessage());
					execution.setCompletedAt(Instant.now());
					execution.setStatus(ReportJobStatus.ERROR);
					execution.setMessage("Job failed");
				}
			}
		}
		finally {
			activeStatements.remove(execution.getExecutionId());
			inFlightKeys.remove(execution.getDedupeKey(), execution.getExecutionId());
		}
		return CompletableFuture.completedFuture(null);
	}

	public ReportJob getJob(String handleId) {
		String executionId = handles.get(handleId);
		if (executionId == null) {
			return null;
		}
		ReportJobExecution execution = executions.get(executionId);
		if (execution == null) {
			return null;
		}
		return toDto(handleId, execution);
	}


	public boolean cancelJob(String handleId) {
		String executionId = handles.get(handleId);
		if (executionId == null) {
			return false;
		}
		ReportJobExecution execution = executions.get(executionId);
		if (execution == null) {
			return false;
		}
		boolean shouldCancelExecution;
		synchronized (execution.getLock()) {
			if (!isActive(execution.getStatus())) {
				return false;
			}
			handles.remove(handleId);
			execution.getSubscriberHandleIds().remove(handleId);
			shouldCancelExecution = execution.getSubscriberHandleIds().isEmpty();
			if (shouldCancelExecution) {
				if (execution.getFuture() != null) {
					execution.getFuture().cancel(true);
				}
				execution.setError("Job cancelled by client");
				execution.setCompletedAt(Instant.now());
				execution.setStatus(ReportJobStatus.ERROR);
				execution.setMessage("Cancelled by client");
				inFlightKeys.remove(execution.getDedupeKey(), executionId);
			}
		}
		if (shouldCancelExecution) {
			CallableStatement stmt = activeStatements.remove(executionId);
			if (stmt != null) {
				try {
					stmt.cancel();
				}
				catch (SQLException e) {
					log.warn("Failed to cancel active DB statement for job " + executionId + ": " + e.getMessage());
				}
			}
		}
		return true;
	}

	@Scheduled(fixedDelay = 600_000)
	public void cleanupExpiredJobs() {
		long ttlSeconds = getJobTtlSeconds();
		Instant cutoff = Instant.now().minusSeconds(ttlSeconds);
		executions.entrySet().removeIf(entry -> {
			ReportJobExecution execution = entry.getValue();
			Instant completedAt = execution.getCompletedAt();
			boolean expired = !isActive(execution.getStatus()) && completedAt != null && completedAt.isBefore(cutoff);
			if (expired) {
				handles.keySet().removeAll(execution.getSubscriberHandleIds());
			}
			return expired;
		});
	}

	private long getJobTtlSeconds() {
		return 1800L;
	}
}
