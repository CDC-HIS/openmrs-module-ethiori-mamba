package org.openmrs.module.mambaetl.datasetdefinition.linelist;

import org.openmrs.module.reporting.dataset.definition.BaseDataSetDefinition;
import org.openmrs.module.reporting.definition.configuration.ConfigurationProperty;
import org.springframework.stereotype.Component;

import java.util.Date;

/**
 * TXNewDataSetDefinitionMamba
 */
@Component
public class ProvidersViewLineListDataSetDefinitionMamba extends BaseDataSetDefinition {

	
	@ConfigurationProperty
	private Date endDate;
	
	@ConfigurationProperty
	private String clientType = "all";
	
	@ConfigurationProperty
	private String patientGUID;

	
	public Date getEndDate() {
		return endDate;
	}
	
	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}
	
	public String getClientType() {
		return clientType;
	}
	
	public void setClientType(String clientType) {
		this.clientType = clientType;
	}
	
	public String getPatientGUID() {
		return patientGUID;
	}
	
	public void setPatientGUID(String patientGUID) {
		this.patientGUID = patientGUID;
	}
}
