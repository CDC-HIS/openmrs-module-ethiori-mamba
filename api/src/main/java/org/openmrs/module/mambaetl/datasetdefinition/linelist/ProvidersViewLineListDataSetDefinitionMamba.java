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
	private Date startDate;
	
	@ConfigurationProperty
	private Date endDate;
	
	@ConfigurationProperty
	private Date appointmentStartDate;
	
	@ConfigurationProperty
	private Date appointmentEndDate;
	
	@ConfigurationProperty
	private String clientType = "all";
	
	@ConfigurationProperty
	private String patientGUID;
	
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
	
	public Date getAppointmentStartDate() {
		return appointmentStartDate;
	}
	
	public void setAppointmentStartDate(Date appointmentStartDate) {
		this.appointmentStartDate = appointmentStartDate;
	}
	
	public Date getAppointmentEndDate() {
		return appointmentEndDate;
	}
	
	public void setAppointmentEndDate(Date appointmentEndDate) {
		this.appointmentEndDate = appointmentEndDate;
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
