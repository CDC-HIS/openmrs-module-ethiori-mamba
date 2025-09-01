package org.openmrs.module.mambaetl.reports.linelist;

import org.openmrs.module.mambaetl.datasetdefinition.linelist.PHRHServiceLineListDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.linelist.PHRHSnsLineListDataSetDefinitionMamba;
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

@Component
public class PHRHSnsLineListReportMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "9e7e33ce-975b-4d1c-a0af-5746ec5bfd18";
	}
	
	@Override
	public String getName() {
		return "LINELIST- People at High risk for HIV Infection SNS";
	}
	
	@Override
	public String getDescription() {
		return "People at High risk for HIV Infection SNS Report";
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
		
		Parameter phrhCode = new Parameter("phrhCode", "PHRH Code", String.class);
		phrhCode.setRequired(false);
		
		return Arrays.asList(startDate, startDateGC, endDate, endDateGC, phrhCode);
		
	}
	
	@Override
	public ReportDefinition constructReportDefinition() {
		ReportDefinition reportDefinition = new ReportDefinition();
		reportDefinition.setUuid(getUuid());
		reportDefinition.setName(getName());
		reportDefinition.setDescription(getDescription());
		reportDefinition.setParameters(getParameters());
		
		PHRHSnsLineListDataSetDefinitionMamba phrhSnsLineListDataSetDefinitionMamba = new PHRHSnsLineListDataSetDefinitionMamba();
		phrhSnsLineListDataSetDefinitionMamba.addParameters(getParameters());
		reportDefinition.addDataSetDefinition("PHRH SNS Report", EthiOhriUtil.map(phrhSnsLineListDataSetDefinitionMamba,
		    "startDate=${startDateGC},endDate=${endDateGC},phrhCode=${phrhCode}"));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("3e5f4159-61d4-4576-8bfb-71ecce1b69d6", reportDefinition);
		
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
