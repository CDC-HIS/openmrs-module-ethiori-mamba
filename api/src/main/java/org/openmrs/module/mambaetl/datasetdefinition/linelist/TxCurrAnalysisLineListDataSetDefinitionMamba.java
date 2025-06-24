package org.openmrs.module.mambaetl.datasetdefinition.linelist;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.module.mambaetl.helpers.reportOptions.TxCurrAnalysisCategories;
import org.openmrs.module.reporting.dataset.definition.BaseDataSetDefinition;
import org.openmrs.module.reporting.definition.configuration.ConfigurationProperty;
import org.springframework.stereotype.Component;

@Component
public class TxCurrAnalysisLineListDataSetDefinitionMamba extends BaseDataSetDefinition {
	
	private static final Log log = LogFactory.getLog(TxCurrAnalysisLineListDataSetDefinitionMamba.class);
	
	@ConfigurationProperty
	private Date startDate;
	
	@ConfigurationProperty
	private Date endDate;
	
	@ConfigurationProperty
	private String txCurrAnalysisCategories;
	
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
	
	public String getTxCurrAnalysisCategories() {
		return txCurrAnalysisCategories;
	}
	
	public void setTxCurrAnalysisCategories(String txCurrAnalysisCategories) {
		this.txCurrAnalysisCategories = txCurrAnalysisCategories;
	}
}
