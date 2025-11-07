package org.openmrs.module.mambaetl.reports.linelist;

import org.openmrs.module.mambaetl.datasetdefinition.linelist.ChildCohortAnalysisLineListDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.linelist.MotherCohortAnalysisLineListDataSetDefinitionMamba;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.ReportRequest;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.reporting.report.manager.ReportManager;
import org.openmrs.module.reporting.report.manager.ReportManagerUtil;
import org.springframework.stereotype.Component;

import java.util.*;

import static org.openmrs.module.mambaetl.helpers.EthiOhriUtil.map;

//@Component
public class ChildCohortAnalysisLineListReportMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "0eab8da6-0000-1111-we34-b173ea4d9c9c";
	}
	
	@Override
	public String getName() {
		return "ART COHORT - Child Cohort Analysis LineList";
	}
	
	@Override
	public String getDescription() {
		return null;
	}
	
	@Override
	public List<Parameter> getParameters() {
		Parameter startDate = new Parameter("startDate", "Start Date", Date.class);
		startDate.setRequired(true);
		Parameter startDateGC = new Parameter("startDateGC", " ", Date.class);
		startDateGC.setRequired(false);
		Parameter endDate = new Parameter("endDate", "End Date", Date.class);
		endDate.setRequired(true);
		Parameter endDateGC = new Parameter("endDateGC", " ", Date.class);
		endDateGC.setRequired(false);
		
		Parameter type = new Parameter("type", "Cohort Report Type", String.class);
		type.setRequired(true);
		type.addToWidgetConfiguration("codedOptions", "SUMMARY");
		type.setDefaultValue("SUMMARY");
		
		return Arrays.asList(startDate, startDateGC, endDate, endDateGC, type);
		
	}
	
	@Override
	public ReportDefinition constructReportDefinition() {
		ReportDefinition reportDefinition = new ReportDefinition();
		reportDefinition.setUuid(getUuid());
		reportDefinition.setName(getName());
		reportDefinition.setDescription(getDescription());
		
		reportDefinition.setParameters(getParameters());
		
		ChildCohortAnalysisLineListDataSetDefinitionMamba dataSetDefinitionMamba = new ChildCohortAnalysisLineListDataSetDefinitionMamba();
		dataSetDefinitionMamba.addParameters(getParameters());
		
		reportDefinition.addDataSetDefinition("Child Cohort Analysis LineList",
		    map(dataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC},type=${type}"));
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		ReportDesign design = ReportManagerUtil.createExcelDesign("d6c499b1-0909-8822-0g00-3141952c4942", reportDefinition);
		design.setReportDefinition(reportDefinition);
		
		return Collections.singletonList(design);
	}
	
	@Override
	public List<ReportRequest> constructScheduledRequests(ReportDefinition reportDefinition) {
		return new ArrayList<>();
	}
	
	@Override
	public String getVersion() {
		return "1.0.0-SNAPSHOT";
	}
}
