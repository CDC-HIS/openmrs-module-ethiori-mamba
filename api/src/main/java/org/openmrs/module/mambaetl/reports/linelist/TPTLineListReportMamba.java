package org.openmrs.module.mambaetl.reports.linelist;

import org.openmrs.module.reporting.report.manager.ReportManager;
import org.openmrs.module.mambaetl.datasetdefinition.linelist.TPTLineListDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.ReportRequest;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.reporting.report.manager.ReportManagerUtil;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

@Component
public class TPTLineListReportMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "f70f5652-c907-487c-aa3a-19b5894498c7";
	}
	
	@Override
	public String getName() {
		return "MAMBA LINELIST- TPT_LINE_LIST";
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
		
		TPTLineListDataSetDefinitionMamba tptLineListDataSetDefinitionMamba = new TPTLineListDataSetDefinitionMamba();
		tptLineListDataSetDefinitionMamba.addParameters(getParameters());
		
		reportDefinition.addDataSetDefinition("List of Patients with TPT",
		    EthiOhriUtil.map(tptLineListDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("158134fb-0d0c-478c-ba44-e5552b90f18f", reportDefinition);
		
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
