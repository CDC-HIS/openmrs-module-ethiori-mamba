package org.openmrs.module.mambaetl.reports.datim;

import org.openmrs.module.mambaetl.datasetdefinition.datim.HeaderDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.pmtct.PmtctArtDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_ml.MLKeyPopulationDataSetDefinitionMamba;
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
public class PMTCTArtReportsMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "2430b29d-0e71-40d1-9da9-71dfb31d146a";
	}
	
	@Override
	public String getName() {
		return "DATIM TREATMENT- PMTCT_ART";
	}
	
	@Override
	public String getDescription() {
		return "PMTCT_ART DATIM mamba report ";
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
		headerDefinition.setDescription("DSD: PMTCT_ART");
		headerDefinition.setParameters(getParameters());
		reportDefinition.addDataSetDefinition("DSD: PMTCT_ART", EthiOhriUtil.map(headerDefinition, "endDate=${endDateGC}"));
		
		PmtctArtDataSetDefinitionMamba pmtctArtDataSetDefinitionMamba = new PmtctArtDataSetDefinitionMamba();
		pmtctArtDataSetDefinitionMamba.addParameters(getParameters());
		pmtctArtDataSetDefinitionMamba.setDescription("Disaggregated by Regimen Type");
		reportDefinition.addDataSetDefinition("Disaggregated by Regimen Type",
		    EthiOhriUtil.map(pmtctArtDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("45b91c33-8f32-4efd-91b6-919eaa738bac", reportDefinition);
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
