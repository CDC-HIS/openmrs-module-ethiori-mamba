package org.openmrs.module.mambaetl.reports.hmis_dhis2;

import org.openmrs.module.mambaetl.datasetdefinition.hmis_dhis2.HMISDHIS2DatasetDefinition;
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
public class HMISDHIS2ReportMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "d17ecb2c-e80a-4e2f-99f2-ebba328739a8";
	}
	
	@Override
	public String getName() {
		return "HMIS- HMIS DHIS 2";
	}
	
	@Override
	public String getDescription() {
		return null;
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
		
		HMISDHIS2DatasetDefinition hmisdhis2DatasetDefinition = new HMISDHIS2DatasetDefinition();
		hmisdhis2DatasetDefinition.addParameters(getParameters());
		
		reportDefinition.addDataSetDefinition(
		    "06 - HIV | Hospital, Health center, Clinic | Monthly (Federal Ministry Of Health) ",
		    EthiOhriUtil.map(hmisdhis2DatasetDefinition, getParameterMappings()));
		return reportDefinition;
	}
	
	private String getParameterMappings() {
		return "startDate=${startDateGC},endDate=${endDateGC}";
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("0c73def9-dc2f-49ff-a852-54d7cda91eb9", reportDefinition);
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
