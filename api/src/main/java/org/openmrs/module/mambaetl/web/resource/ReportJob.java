package org.openmrs.module.mambaetl.web.resource;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ReportJob {
	
	private final String jobId;
	
	private volatile ReportJobStatus status;
	
	private final String procedureName;
	
	private final Instant submittedAt;
	
	private volatile Instant completedAt;
	
	private volatile ReportDataResponse result;
	
	private volatile String error;
	
	@JsonIgnore
	private volatile Future<?> future;
	
	public ReportJob(String jobId, String procedureName) {
		this.jobId = jobId;
		this.procedureName = procedureName;
		this.submittedAt = Instant.now();
		this.status = ReportJobStatus.PENDING;
	}
	
	public String getJobId() {
		return jobId;
	}
	
	public ReportJobStatus getStatus() {
		return status;
	}
	
	public void setStatus(ReportJobStatus status) {
		this.status = status;
	}
	
	public String getProcedureName() {
		return procedureName;
	}
	
	public String getSubmittedAt() {
		return submittedAt.toString();
	}
	
	@JsonProperty("completedAt")
	public String getCompletedAtFormatted() {
		return completedAt != null ? completedAt.toString() : null;
	}
	
	// Used internally for TTL comparisons — not serialized
	@JsonIgnore
	public Instant getCompletedAtInstant() {
		return completedAt;
	}
	
	public void setCompletedAt(Instant completedAt) {
		this.completedAt = completedAt;
	}
	
	// Present only while PENDING or RUNNING
	public Long getElapsedMs() {
		ReportJobStatus s = this.status;
		if (s == ReportJobStatus.PENDING || s == ReportJobStatus.RUNNING) {
			return Instant.now().toEpochMilli() - submittedAt.toEpochMilli();
		}
		return null;
	}
	
	// Present only when COMPLETE
	public Integer getRowCount() {
		return result != null ? result.getRowCount() : null;
	}
	
	// Present only when COMPLETE
	public List<Map<String, Object>> getData() {
		return result != null ? result.getData() : null;
	}
	
	public void setResult(ReportDataResponse result) {
		this.result = result;
	}
	
	public String getError() {
		return error;
	}
	
	public void setError(String error) {
		this.error = error;
	}
	
	@JsonIgnore
	public Future<?> getFuture() {
		return future;
	}
	
	public void setFuture(Future<?> future) {
		this.future = future;
	}
}
