package org.openmrs.module.mambaetl.reports.linelist.migrated;

import java.sql.Date;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.openmrs.module.mambaetl.datasetdefinition.linelist.dataExtractionTool.VlEligibilityDataExtractionDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.ReportRequest;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.reporting.report.manager.ReportManager;
import org.openmrs.module.reporting.report.manager.ReportManagerUtil;
import org.springframework.stereotype.Component;

@Component
public class VlEligibilityDataExtractionReport implements ReportManager {
	
	@Override
	public String getUuid() {
		return "33df3be4-95ef-48ce-aa3f-90a12573a8e2";
	}
	
	@Override
	public String getName() {
		return "MAMBA LINELIST- Vl_Eligibility_Data_Extraction";
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
		
		VlEligibilityDataExtractionDataSetDefinitionMamba vlEligibilityDataExtractionDatasetDefinition = new VlEligibilityDataExtractionDataSetDefinitionMamba();
		vlEligibilityDataExtractionDatasetDefinition.addParameters(getParameters());
		
		reportDefinition.addDataSetDefinition("List of Patients for Vl Eligibility Extraction",
		    EthiOhriUtil.map(vlEligibilityDataExtractionDatasetDefinition, "startDate=${startDateGC},endDate=${endDateGC}"));
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("36f9445d-8a00-41da-8ef2-e58b19976f34", reportDefinition);
		
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
