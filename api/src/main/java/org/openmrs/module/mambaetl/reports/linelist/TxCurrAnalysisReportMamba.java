package org.openmrs.module.mambaetl.reports.linelist;

import java.sql.Date;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.openmrs.module.mambaetl.datasetdefinition.linelist.TxCurrAnalysisDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.ReportRequest;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.reporting.report.manager.ReportManager;
import org.openmrs.module.reporting.report.manager.ReportManagerUtil;
import org.springframework.stereotype.Component;

@Component
public class TxCurrAnalysisReportMamba implements ReportManager {
	
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
		
		TxCurrAnalysisDataSetDefinitionMamba txCurrAnalysisDataSetDefinition = new TxCurrAnalysisDataSetDefinitionMamba();
		txCurrAnalysisDataSetDefinition.addParameters(getParameters());
		
		reportDefinition.addDataSetDefinition("List of Patients for TX Curr Analysis",
		    EthiOhriUtil.map(txCurrAnalysisDataSetDefinition, "startDate=${startDateGC},endDate=${endDateGC}"));
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("5a289fef-38ce-4635-8a51-422ccf599b14", reportDefinition);
		
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
