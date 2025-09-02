package org.openmrs.module.mambaetl.reports.linelist;

import org.openmrs.module.mambaetl.datasetdefinition.linelist.ProvidersViewLineListDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.ReportRequest;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.reporting.report.manager.ReportManager;
import org.openmrs.module.reporting.report.manager.ReportManagerUtil;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class ProvidersViewLineListReportMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "c277163b-98be-41db-bb06-57114b0b6cfa";
	}
	
	@Override
	public String getName() {
		return "LINELIST- Data Quality & Data Use";
	}
	
	@Override
	public String getDescription() {
		return "Data Quality & Data Use";
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
		
		Parameter clientType = new Parameter("clientType", "Client Type", String.class);
		clientType.setRequired(true);
		clientType.addToWidgetConfiguration("codedOptions", "ALL,TX_CURR");
		clientType.setDefaultValue("ALL");
		
		Parameter patientGUID = new Parameter("patientGUID", "Patient GUID", String.class);
		patientGUID.setRequired(false);
		//
		
		return Arrays.asList(startDate, startDateGC, endDate, endDateGC, clientType, patientGUID);
		
	}
	
	@Override
	public ReportDefinition constructReportDefinition() {
		ReportDefinition reportDefinition = new ReportDefinition();
		reportDefinition.setUuid(getUuid());
		reportDefinition.setName(getName());
		reportDefinition.setDescription(getDescription());
		
		reportDefinition.setParameters(getParameters());
		
		ProvidersViewLineListDataSetDefinitionMamba providersViewLineListDataSetDefinitionMamba = new ProvidersViewLineListDataSetDefinitionMamba();
		providersViewLineListDataSetDefinitionMamba.addParameters(getParameters());
		
		reportDefinition.addDataSetDefinition("Data Quality & Data Use", EthiOhriUtil.map(
		    providersViewLineListDataSetDefinitionMamba,
		    "startDate=${startDateGC},endDate=${endDateGC},clientType=${clientType},patientGUID=${patientGUID}"));
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("809de923-d086-4dfb-8786-f02169841b46", reportDefinition);
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
