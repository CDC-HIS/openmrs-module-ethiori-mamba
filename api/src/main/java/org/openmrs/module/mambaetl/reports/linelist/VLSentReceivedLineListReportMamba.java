package org.openmrs.module.mambaetl.reports.linelist;

import org.openmrs.module.mambaetl.datasetdefinition.linelist.VLEligibilityLineListDatasetDefinition;
import org.openmrs.module.mambaetl.datasetdefinition.linelist.VlSentReceivedLineListDataSetDefinitionMamba;
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
public class VLSentReceivedLineListReportMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "103f6cd6-06a1-449c-b29b-a328c5da7580";
	}
	
	@Override
	public String getName() {
		return "LINELIST- VL Sent/Received Line List";
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

		Parameter type = new Parameter("type", "Type", String.class);
		type.setRequired(true);
		type.addToWidgetConfiguration("codedOptions", "Sent,Received");
		type.setDefaultValue("all");

		return Arrays.asList(startDate, startDateGC, endDate, endDateGC, type);
		
	}
	
	@Override
	public ReportDefinition constructReportDefinition() {
		ReportDefinition reportDefinition = new ReportDefinition();
		reportDefinition.setUuid(getUuid());
		reportDefinition.setName(getName());
		reportDefinition.setDescription(getDescription());
		
		reportDefinition.setParameters(getParameters());
		
		VlSentReceivedLineListDataSetDefinitionMamba vlSentReceivedLineListDataSetDefinitionMamba = new VlSentReceivedLineListDataSetDefinitionMamba();
		vlSentReceivedLineListDataSetDefinitionMamba.addParameters(getParameters());
		
		reportDefinition.addDataSetDefinition("VL Sent/Received Line List",
		    EthiOhriUtil.map(vlSentReceivedLineListDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC},type=${type}"));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("56f0d90e-f32f-4f29-bf96-572ed657a0d9", reportDefinition);
		
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
