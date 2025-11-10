package org.openmrs.module.mambaetl.reports.linelist;

import org.apache.xpath.operations.Bool;
import org.openmrs.module.mambaetl.datasetdefinition.linelist.ARTRetentionDataSetDefinitionMamba;
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
public class RetentionLineListReportMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "3d948f9d-7a7f-495a-8063-d756792366ed";
	}
	
	@Override
	public String getName() {
		return "LINELIST- ART Retention";
	}
	
	@Override
	public String getDescription() {
		return "ART Retention line list report";
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
		
		ARTRetentionDataSetDefinitionMamba retentionDataSetDefinitionMamba = new ARTRetentionDataSetDefinitionMamba();
		retentionDataSetDefinitionMamba.addParameters(getParameters());
		reportDefinition.addDataSetDefinition("List of Patients with ART Retention",
		    EthiOhriUtil.map(retentionDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("99887cb8-98fa-474b-ab93-17d73e431ad6", reportDefinition);
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
