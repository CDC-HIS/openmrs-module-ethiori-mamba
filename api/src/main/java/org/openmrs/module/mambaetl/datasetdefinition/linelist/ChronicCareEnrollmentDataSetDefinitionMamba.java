package org.openmrs.module.mambaetl.datasetdefinition.linelist;

import org.openmrs.module.reporting.dataset.definition.BaseDataSetDefinition;
import org.openmrs.module.reporting.definition.configuration.ConfigurationProperty;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class ChronicCareEnrollmentDataSetDefinitionMamba extends BaseDataSetDefinition {
	
	@ConfigurationProperty
	private Date startDate;
	
	@ConfigurationProperty
	private Date endDate;
	
	@ConfigurationProperty
	private String followupStatus;
	
	public String getFollowupStatus() {
		return followupStatus;
	}
	
	public void setFollowupStatus(String followupStatus) {
		this.followupStatus = followupStatus;
	}
	
	public Date getStartDate() {
		return startDate;
	}
	
	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}
	
	public Date getEndDate() {
		return endDate;
	}
	
	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}
	
}
