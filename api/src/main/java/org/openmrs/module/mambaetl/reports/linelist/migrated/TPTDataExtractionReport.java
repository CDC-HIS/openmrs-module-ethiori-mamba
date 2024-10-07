package org.openmrs.module.mambaetl.reports.linelist.migrated;

import org.openmrs.module.reporting.report.manager.ReportManager;
import org.openmrs.module.mambaetl.datasetdefinition.linelist.dataExtractionTool.TPTDataExtractionDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.ReportRequest;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.reporting.report.manager.ReportManagerUtil;
import org.springframework.stereotype.Component;

import java.sql.Date;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Component
public class TPTDataExtractionReport implements ReportManager {
	
	@Override
	public String getUuid() {
		return "9000965c-89d2-490e-abaa-2c5c72770421";
	}
	
	@Override
	public String getName() {
		return "MAMBA LINELIST- TPT_DATA_EXTRACTION";
	}
	
	@Override
	public String getDescription() {
		return null;
	}
	
	@Override
	public List<Parameter> getParameters() {
		Parameter startDate = new Parameter("startDate", "On Month", Date.class);
		startDate.setRequired(false);
		Parameter startDateGC = new Parameter("startDateGC", " ", Date.class);
		startDateGC.setRequired(false);
		Parameter endDate = new Parameter("endDate", "On Month", Date.class);
		endDate.setRequired(false);
		Parameter endDateGC = new Parameter("endDateGC", " ", Date.class);
		endDateGC.setRequired(false);
		return Arrays.asList(startDate, startDateGC, endDate, endDateGC);
	}
	
	@Override
	public ReportDefinition constructReportDefinition() {
		ReportDefinition reportDefinition = new ReportDefinition();
		reportDefinition.setUuid(getUuid());
		reportDefinition.setName(getName());
		reportDefinition.setDescription(getDescription());
		
		reportDefinition.setParameters(getParameters());
		
		TPTDataExtractionDataSetDefinitionMamba tptDataExtractionDataSetDefinitionMamba = new TPTDataExtractionDataSetDefinitionMamba();
		tptDataExtractionDataSetDefinitionMamba.addParameters(getParameters());
		
		reportDefinition.addDataSetDefinition("List of Patients with TPT",
		    EthiOhriUtil.map(tptDataExtractionDataSetDefinitionMamba));
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("77dcf287-1665-4f77-b02e-c920c4bf60e6", reportDefinition);
		
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
