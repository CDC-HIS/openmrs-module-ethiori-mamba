package org.openmrs.module.mambaetl.reports.linelist;

import java.util.Collections;
import java.util.List;

import org.openmrs.module.mambaetl.datasetdefinition.linelist.TxCurrAnalysisLineListDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.ReportRequest;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.reporting.report.manager.ReportManager;
import org.openmrs.module.reporting.report.manager.ReportManagerUtil;
import org.springframework.stereotype.Component;

@Component
public class TxCurrAnalysisLineListReportMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "73528f40-7987-482b-bc49-2d32451d00d9";
	}
	
	@Override
	public String getName() {
		return "MAMBA LINELIST- TX_CURR_ANALYSIS";
	}
	
	@Override
	public String getDescription() {
		return null;
	}
	
	@Override
	public List<Parameter> getParameters() {
		return EthiOhriUtil.getDateRangeParameters();
		
	}
	
	@Override
	public ReportDefinition constructReportDefinition() {
		ReportDefinition reportDefinition = new ReportDefinition();
		reportDefinition.setUuid(getUuid());
		reportDefinition.setName(getName());
		reportDefinition.setDescription(getDescription());
		
		reportDefinition.setParameters(getParameters());
		
		TxCurrAnalysisLineListDataSetDefinitionMamba txCurrAnalysisDataSetDefinition = new TxCurrAnalysisLineListDataSetDefinitionMamba();
		txCurrAnalysisDataSetDefinition.addParameters(getParameters());
		
		reportDefinition.addDataSetDefinition("List of Patients for TX Curr Analysis", EthiOhriUtil.map(
		    txCurrAnalysisDataSetDefinition,
		    "startDate=${startDateGC},endDate=${endDateGC},txCurrAnalysisCategory=${txCurrAnalysisCategory}"));
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("ef7db8b4-aabd-4c57-a3f9-7cfb6aac7e3a", reportDefinition);
		
		return Collections.singletonList(design);
	}
	
	@Override
	public List<ReportRequest> constructScheduledRequests(ReportDefinition reportDefinition) {
		return null;
	}
	
	@Override
	public String getVersion() {
		return "1.0.0-SNAPSHOT";
	}
}
