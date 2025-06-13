package org.openmrs.module.mambaetl.reports.linelist;

import org.openmrs.module.mambaetl.datasetdefinition.linelist.AHDLineListDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.linelist.TxCurrLineListDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.ReportRequest;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.reporting.report.manager.ReportManager;
import org.openmrs.module.reporting.report.manager.ReportManagerUtil;
import org.springframework.stereotype.Component;

import java.util.*;

import static org.openmrs.module.mambaetl.helpers.EthiOhriUtil.map;

@Component
public class AHDLineListReportMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "691327fb-9579-4c18-920a-27ee7655ebef";
	}
	
	@Override
	public String getName() {
		return "MAMBA LINELIST- AHD";
	}
	
	@Override
	public String getDescription() {
		return null;
	}
	
	@Override
	public List<Parameter> getParameters() {
		
		return EthiOhriUtil.getEndDateParameters(Boolean.FALSE);
		
	}
	
	@Override
	public ReportDefinition constructReportDefinition() {
		ReportDefinition reportDefinition = new ReportDefinition();
		reportDefinition.setUuid(getUuid());
		reportDefinition.setName(getName());
		reportDefinition.setDescription(getDescription());
		
		reportDefinition.setParameters(getParameters());
		
		AHDLineListDataSetDefinitionMamba ahdLineListDataSetDefinitionMamba = new AHDLineListDataSetDefinitionMamba();
		ahdLineListDataSetDefinitionMamba.addParameters(getParameters());
		
		reportDefinition.addDataSetDefinition("AHD line list",
		    map(ahdLineListDataSetDefinitionMamba, "endDate=${endDateGC}"));
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		ReportDesign design = ReportManagerUtil.createExcelDesign("a770d35a-508f-4910-a1dc-8149b7696817", reportDefinition);
		
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
