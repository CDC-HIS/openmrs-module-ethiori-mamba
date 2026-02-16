package org.openmrs.module.mambaetl.reports.linelist;

import org.openmrs.module.mambaetl.datasetdefinition.linelist.NCDTreatmentLineListDataSetDefinitionMamba;
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
public class NCDTreatmentLineListReportMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "3c8e0548-f7bd-4763-800c-dd0e46368ec5";
	}
	
	@Override
	public String getName() {
		return "LINELIST- NCD Treatment ";
	}
	
	@Override
	public String getDescription() {
		return "NCD Treatment Line List Report";
	}
	
	@Override
	public List<Parameter> getParameters() {
		return EthiOhriUtil.getDateRangeParameters(Boolean.FALSE);
		
	}
	
	@Override
	public ReportDefinition constructReportDefinition() {
		ReportDefinition reportDefinition = new ReportDefinition();
		reportDefinition.setUuid(getUuid());
		reportDefinition.setName(getName());
		reportDefinition.setDescription(getDescription());
		reportDefinition.setParameters(getParameters());
		
		NCDTreatmentLineListDataSetDefinitionMamba ncdTreatmentLineListDataSetDefinitionMamba = new NCDTreatmentLineListDataSetDefinitionMamba();
		ncdTreatmentLineListDataSetDefinitionMamba.addParameters(getParameters());
		reportDefinition.addDataSetDefinition("NCD Treatment Line List Report",
		    EthiOhriUtil.map(ncdTreatmentLineListDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("17f76b6b-3c6d-4961-8e08-186a3dd41c99", reportDefinition);
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
