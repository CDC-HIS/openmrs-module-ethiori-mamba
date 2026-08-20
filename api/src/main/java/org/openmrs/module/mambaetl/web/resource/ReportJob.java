package org.openmrs.module.mambaetl.web.resource;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ReportJob {
	
	private final String jobId;
	
	private final ReportJobStatus status;
	
	private final String procedureName;
	
	private final Instant submittedAt;
	
	private final Instant completedAt;
	
	private final ReportDataResponse result;
	
	private final String error;
	
	private final String message;
	
	private final Integer totalSteps;
	
	private final Integer completedSteps;
	
	public ReportJob(String jobId, ReportJobStatus status, String procedureName, Instant submittedAt, Instant completedAt,
	    ReportDataResponse result, String error, String message, Integer totalSteps, Integer completedSteps) {
		this.jobId = jobId;
		this.status = status;
		this.procedureName = procedureName;
		this.submittedAt = submittedAt;
		this.completedAt = completedAt;
		this.result = result;
		this.error = error;
		this.message = message;
		this.totalSteps = totalSteps;
		this.completedSteps = completedSteps;
	}
	
	public String getJobId() {
		return jobId;
	}
	
	public ReportJobStatus getStatus() {
		return status;
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
	
	public Long getElapsedMs() {
		if (status == ReportJobStatus.PENDING || status == ReportJobStatus.RUNNING) {
			return Instant.now().toEpochMilli() - submittedAt.toEpochMilli();
		}
		return null;
	}
	
	public Integer getRowCount() {
		return result != null ? result.getRowCount() : null;
	}
	
	public List<Map<String, Object>> getData() {
		return result != null ? result.getData() : null;
	}
	
	public String getError() {
		return error;
	}
	
	public String getMessage() {
		return message;
	}
	
	public Integer getTotalSteps() {
		return totalSteps;
	}
	
	public Integer getCompletedSteps() {
		return completedSteps;
	}
	
	public Integer getProgressPercent() {
		if (totalSteps == null || totalSteps <= 0) {
			return null;
		}
		return ((completedSteps == null ? 0 : completedSteps) * 100) / totalSteps;
	}
}
