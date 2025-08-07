package org.openmrs.module.mambaetl.datasetdefinition.linelist;

import org.openmrs.module.mambaetl.reports.linelist.TXTBReportMamba;
import org.openmrs.module.reporting.dataset.definition.BaseDataSetDefinition;
import org.openmrs.module.reporting.definition.configuration.ConfigurationProperty;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class TXTBDataSetDefinitionMamba extends BaseDataSetDefinition {
	
	@ConfigurationProperty
	private Date startDate;
	
	@ConfigurationProperty
	private Date endDate;
	
	@ConfigurationProperty
	private String type;
	
	public String getType() {
		if (type == null) {
			return TXTBReportMamba.tb_art;
		}
		return type;
	}
	
	public void setType(String type) {
		this.type = type;
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
