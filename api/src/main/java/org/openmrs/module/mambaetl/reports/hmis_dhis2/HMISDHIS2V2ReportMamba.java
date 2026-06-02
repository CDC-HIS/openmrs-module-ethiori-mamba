package org.openmrs.module.mambaetl.reports.hmis_dhis2;

import org.openmrs.module.mambaetl.datasetdefinition.hmis_dhis2.HMISDHIS2V2DatasetDefinition;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.ReportRequest;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.reporting.report.manager.ReportManager;
import org.openmrs.module.reporting.report.manager.ReportManagerUtil;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

@Component
public class HMISDHIS2V2ReportMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "b5e2a4c8-3f1d-4e7b-9c6a-8d0f2e1b3c5a";
	}
	
	@Override
	public String getName() {
		return "HMIS- HMIS DHIS 2 (v2 Optimized)";
	}
	
	@Override
	public String getDescription() {
		return "Optimized version using a single shared temporary table to reduce file descriptor usage on large datasets";
	}
	
	@Override
	public List<Parameter> getParameters() {
		return EthiOhriUtil.getDateRangeParameters(Boolean.TRUE);
	}
	
	@Override
	public ReportDefinition constructReportDefinition() {
		ReportDefinition reportDefinition = new ReportDefinition();
		reportDefinition.setUuid(getUuid());
		reportDefinition.setName(getName());
		reportDefinition.setDescription(getDescription());
		
		reportDefinition.setParameters(getParameters());
		
		HMISDHIS2V2DatasetDefinition datasetDefinition = new HMISDHIS2V2DatasetDefinition();
		datasetDefinition.addParameters(getParameters());
		
		reportDefinition.addDataSetDefinition(
		    "06 - HIV | Hospital, Health center, Clinic | Monthly (Federal Ministry Of Health) ",
		    EthiOhriUtil.map(datasetDefinition, getParameterMappings()));
		return reportDefinition;
	}
	
	private String getParameterMappings() {
		return "startDate=${startDateGC},endDate=${endDateGC}";
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		ReportDesign design = ReportManagerUtil.createExcelDesign("a3f8e2c1-6d4b-4e7f-9b5a-2c0d8f1e4a7b", reportDefinition);
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
