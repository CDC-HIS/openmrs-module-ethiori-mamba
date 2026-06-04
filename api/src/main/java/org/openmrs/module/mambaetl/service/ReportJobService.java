package org.openmrs.module.mambaetl.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.module.mambaetl.config.AsyncConfig;
import org.openmrs.module.mambaetl.web.resource.ReportDataResponse;
import org.openmrs.module.mambaetl.web.resource.ReportJob;
import org.openmrs.module.mambaetl.web.resource.ReportJobStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Service
public class ReportJobService {

    private static final Log log = LogFactory.getLog(ReportJobService.class);

    private static final long DEFAULT_TTL_SECONDS = 1800L;

    private final ConcurrentHashMap<String, ReportJob> jobs = new ConcurrentHashMap<>();

    @Autowired
    private DynamicReportExecutorService reportExecutorService;

    private volatile boolean cleanupScheduled = false;

    public ReportJob submitJob(String procedureName, Map<String, String> params, int offset, int limit) {
        String jobId = UUID.randomUUID().toString();
        ReportJob job = new ReportJob(jobId, procedureName);
        jobs.put(jobId, job);
        try {
            CompletableFuture<Void> future = CompletableFuture.runAsync(
                    () -> executeJobSync(job, params, offset, limit),
                    AsyncConfig.getReportExecutor());
            job.setFuture(future);
            scheduleCleanupIfNeeded();
        } catch (Exception e) {
            log.error("Failed to queue report job " + jobId, e);
            synchronized (job) {
                job.setError("Failed to queue job: " + e.getMessage());
                job.setCompletedAt(Instant.now());
                job.setStatus(ReportJobStatus.ERROR);
            }
        }
        return job;
    }

    private void executeJobSync(ReportJob job, Map<String, String> params, int offset, int limit) {
        synchronized (job) {
            job.setStatus(ReportJobStatus.RUNNING);
        }
        try {
            DynamicReportExecutorService.ReportExecutionResult result = reportExecutorService.executeReport(
                    job.getProcedureName(), params, offset, limit, (completed, total) -> {
                        synchronized (job) {
                            job.setTotalSteps(total);
                            job.setCompletedSteps(completed);
                        }
                    });
            synchronized (job) {
                job.setResult(new ReportDataResponse(job.getProcedureName(), result.getData()));
                job.setCompletedAt(Instant.now());
                job.setStatus(ReportJobStatus.COMPLETE);
            }
        } catch (Exception e) {
            log.error("Async report job failed for procedure " + job.getProcedureName(), e);
            synchronized (job) {
                job.setError("Stored procedure execution failed: " + e.getMessage());
                job.setCompletedAt(Instant.now());
                job.setStatus(ReportJobStatus.ERROR);
            }
        }
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

    private synchronized void scheduleCleanupIfNeeded() {
        if (cleanupScheduled) {
            return;
        }
        cleanupScheduled = true;
        AsyncConfig.getCleanupScheduler().scheduleAtFixedRate(this::cleanupExpiredJobs,
                10, 600, TimeUnit.SECONDS);
    }

    void cleanupExpiredJobs() {
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
        try {
            String val = org.openmrs.api.context.Context.getAdministrationService()
                    .getGlobalProperty("mambaetl.report.job.ttl.seconds");
            if (val != null && !val.trim().isEmpty()) {
                return Long.parseLong(val.trim());
            }
        } catch (Exception e) {
            log.warn("Could not read mambaetl.report.job.ttl.seconds, using default 1800 s", e);
        }
        return DEFAULT_TTL_SECONDS;
    }
}