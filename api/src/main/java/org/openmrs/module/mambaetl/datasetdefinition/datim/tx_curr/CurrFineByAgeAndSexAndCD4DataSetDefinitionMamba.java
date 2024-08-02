package org.openmrs.module.mambaetl.datasetdefinition.datim.tx_curr;

import org.openmrs.module.mambaetl.helpers.mapper.Cd4Status;
import org.openmrs.module.reporting.dataset.definition.BaseDataSetDefinition;
import org.openmrs.module.reporting.definition.configuration.ConfigurationProperty;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class CurrFineByAgeAndSexAndCD4DataSetDefinitionMamba extends BaseDataSetDefinition {
	
	@ConfigurationProperty
	private Date startDate;
	
	public Date getStartDate() {
		return startDate;
	}
	
	@ConfigurationProperty
	private Date endDate;
	
	@ConfigurationProperty
	private Cd4Status cd4Status = Cd4Status.UNKNOWN;
	
	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}
	
	public Date getEndDate() {
		return endDate;
	}
	
	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}
	
	public Cd4Status getCd4Status() {
		return cd4Status;
	}
	
	public void setCd4Status(Cd4Status cd4Status) {
		this.cd4Status = cd4Status;
	}
}
