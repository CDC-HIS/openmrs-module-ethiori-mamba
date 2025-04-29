package org.openmrs.module.mambaetl.datasetdefinition.datim.tb_art;

import org.openmrs.module.mambaetl.helpers.reportOptions.TBArtAggregationTypes;
import org.openmrs.module.reporting.dataset.definition.BaseDataSetDefinition;
import org.openmrs.module.reporting.definition.configuration.ConfigurationProperty;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class TBArtDataSetDefinitionMamba extends BaseDataSetDefinition {
	
	@ConfigurationProperty
	private Date startDate;
	
	@ConfigurationProperty
	private Date endDate;
	
	@ConfigurationProperty
	private TBArtAggregationTypes tbArtAggregationTypes;
	
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
	
	public TBArtAggregationTypes getTbArtAggregationTypes() {
		return tbArtAggregationTypes;
	}
	
	public void setTbArtAggregationTypes(TBArtAggregationTypes tbArtAggregationTypes) {
		this.tbArtAggregationTypes = tbArtAggregationTypes;
	}
}
