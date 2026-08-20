package org.openmrs.module.mambaetl.service;

import org.openmrs.module.mambaetl.web.resource.ReportDataResponse;
import org.openmrs.module.mambaetl.web.resource.ReportJobStatus;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

public class ReportJobExecution {
	
	private final String executionId;
	
	private final String procedureName;
	
	private final String dedupeKey;
	
	private final Instant submittedAt = Instant.now();
	
	private final Set<String> subscriberHandleIds = ConcurrentHashMap.newKeySet();
	
	// Dedicated monitor for compound status transitions (e.g. "check status, then set status
	// and result"). Locking on this instead of the execution instance itself keeps the lock
	// private to this package, rather than a shared object other code could accidentally
	// synchronize on too.
	private final Object lock = new Object();
	
	private volatile ReportJobStatus status = ReportJobStatus.PENDING;
	
	private volatile Instant completedAt;
	
	private volatile ReportDataResponse result;
	
	private volatile String error;
	
	private volatile String message = "Queued, waiting for executor thread";
	
	private volatile Future<?> future;
	
	private volatile Integer totalSteps;
	
	private volatile Integer completedSteps;
	
	ReportJobExecution(String executionId, String procedureName, String dedupeKey) {
		this.executionId = executionId;
		this.procedureName = procedureName;
		this.dedupeKey = dedupeKey;
	}
	
	String getExecutionId() {
		return executionId;
	}
	
	String getProcedureName() {
		return procedureName;
	}
	
	String getDedupeKey() {
		return dedupeKey;
	}
	
	Instant getSubmittedAt() {
		return submittedAt;
	}
	
	Set<String> getSubscriberHandleIds() {
		return subscriberHandleIds;
	}
	
	Object getLock() {
		return lock;
	}
	
	ReportJobStatus getStatus() {
		return status;
	}
	
	void setStatus(ReportJobStatus status) {
		this.status = status;
	}
	
	Instant getCompletedAt() {
		return completedAt;
	}
	
	void setCompletedAt(Instant completedAt) {
		this.completedAt = completedAt;
	}
	
	ReportDataResponse getResult() {
		return result;
	}
	
	void setResult(ReportDataResponse result) {
		this.result = result;
	}
	
	String getError() {
		return error;
	}
	
	void setError(String error) {
		this.error = error;
	}
	
	String getMessage() {
		return message;
	}
	
	void setMessage(String message) {
		this.message = message;
	}
	
	Future<?> getFuture() {
		return future;
	}
	
	void setFuture(Future<?> future) {
		this.future = future;
	}
	
	Integer getTotalSteps() {
		return totalSteps;
	}
	
	void setTotalSteps(Integer totalSteps) {
		this.totalSteps = totalSteps;
	}
	
	Integer getCompletedSteps() {
		return completedSteps;
	}
	
	void setCompletedSteps(Integer completedSteps) {
		this.completedSteps = completedSteps;
	}
}
