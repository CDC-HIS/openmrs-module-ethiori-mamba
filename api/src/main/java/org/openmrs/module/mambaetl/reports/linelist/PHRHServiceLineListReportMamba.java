package org.openmrs.module.mambaetl.reports.linelist;

import org.openmrs.module.mambaetl.datasetdefinition.linelist.PHRHServiceLineListDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.ReportRequest;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.reporting.report.manager.ReportManager;
import org.openmrs.module.reporting.report.manager.ReportManagerUtil;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

//@Component
public class PHRHServiceLineListReportMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "00234c6f-79b9-44a4-9628-d4aeaec93850";
	}
	
	@Override
	public String getName() {
		return "LINELIST- People at High risk for HIV Infection Service";
	}
	
	@Override
	public String getDescription() {
		return "People at High risk for HIV Infection Service Report";
	}
	
	@Override
	public List<Parameter> getParameters() {
		Parameter startDate = new Parameter("startDate", "Start Date", Date.class);
		startDate.setRequired(false);
		Parameter startDateGC = new Parameter("startDateGC", " ", Date.class);
		startDateGC.setRequired(false);
		
		Parameter endDate = new Parameter("endDate", "End Date", Date.class);
		endDate.setRequired(false);
		Parameter endDateGC = new Parameter("endDateGC", " ", Date.class);
		endDateGC.setRequired(false);
		
		Parameter targetGroup = new Parameter("targetGroup", "Target Group", String.class);
		targetGroup.setRequired(false);
		targetGroup.addToWidgetConfiguration("codedOptions", "ALL,FSW,PWID,High Risk AGYW,Other KPP,General Population");
		targetGroup.setDefaultValue("ALL");
		return Arrays.asList(startDate, startDateGC, endDate, endDateGC, targetGroup);
		
	}
	
	@Override
	public ReportDefinition constructReportDefinition() {
		ReportDefinition reportDefinition = new ReportDefinition();
		reportDefinition.setUuid(getUuid());
		reportDefinition.setName(getName());
		reportDefinition.setDescription(getDescription());
		reportDefinition.setParameters(getParameters());
		
		PHRHServiceLineListDataSetDefinitionMamba phrhServiceLineListDataSetDefinitionMamba = new PHRHServiceLineListDataSetDefinitionMamba();
		phrhServiceLineListDataSetDefinitionMamba.addParameters(getParameters());
		reportDefinition.addDataSetDefinition("PHRH Service Report", EthiOhriUtil.map(
		    phrhServiceLineListDataSetDefinitionMamba,
		    "startDate=${startDateGC},endDate=${endDateGC},targetGroup=${targetGroup}"));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("86ba6d09-e0ee-4d59-a7e1-614793a380f9", reportDefinition);
		design.setReportDefinition(reportDefinition);
		
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
