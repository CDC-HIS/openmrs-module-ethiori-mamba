package org.openmrs.module.mambaetl.datasetdefinition.datim.tx_new;

import org.openmrs.module.mambaetl.helpers.mapper.Cd4Status;
import org.openmrs.module.reporting.dataset.definition.BaseDataSetDefinition;
import org.openmrs.module.reporting.definition.configuration.ConfigurationProperty;

import java.util.Date;

public class HeaderDataSetDefinitionMamba extends BaseDataSetDefinition {
	
	@ConfigurationProperty
	private Date startDate;
	
	public Date getStartDate() {
		return startDate;
	}
	
	@ConfigurationProperty
	private Date endDate;
	
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
