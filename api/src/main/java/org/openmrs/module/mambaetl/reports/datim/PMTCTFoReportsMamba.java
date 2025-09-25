package org.openmrs.module.mambaetl.reports.datim;

import org.openmrs.module.mambaetl.datasetdefinition.datim.HeaderDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.pmtct.PmtctFoDataSetDefinitionMamba;
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
public class PMTCTFoReportsMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "1f9de747-db69-4e79-960f-5fc15ff13ea5";
	}
	
	@Override
	public String getName() {
		return "DATIM TREATMENT- PMTCT_FO";
	}
	
	@Override
	public String getDescription() {
		return "PMTCT_FO DATIM mamba report ";
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
		
		HeaderDataSetDefinitionMamba headerDefinition = new HeaderDataSetDefinitionMamba();
		headerDefinition.setDescription("DSD: PMTCT_FO");
		headerDefinition.setParameters(getParameters());
		reportDefinition.addDataSetDefinition("DSD: PMTCT_FO", EthiOhriUtil.map(headerDefinition, "endDate=${endDateGC}"));
		
		PmtctFoDataSetDefinitionMamba pmtctFoDataSetDefinitionMamba = new PmtctFoDataSetDefinitionMamba();
		pmtctFoDataSetDefinitionMamba.addParameters(getParameters());
		pmtctFoDataSetDefinitionMamba.setDescription("Numerator: Disaggregated by Outcome Type");
		reportDefinition.addDataSetDefinition("Numerator: Disaggregated by Outcome Type",
		    EthiOhriUtil.map(pmtctFoDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("f89d69a0-9222-4d2f-88a1-8eafb82d48f3", reportDefinition);
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
