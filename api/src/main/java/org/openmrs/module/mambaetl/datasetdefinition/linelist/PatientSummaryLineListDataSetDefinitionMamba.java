package org.openmrs.module.mambaetl.datasetdefinition.linelist;

import org.openmrs.module.reporting.dataset.definition.BaseDataSetDefinition;
import org.openmrs.module.reporting.definition.configuration.ConfigurationProperty;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class PatientSummaryLineListDataSetDefinitionMamba extends BaseDataSetDefinition {
	
	@ConfigurationProperty
	private String patientUUID;
	
	public String getPatientUUID() {
		return patientUUID;
	}
	
	public void setPatientUUID(String patientUUID) {
		this.patientUUID = patientUUID;
	}
}
