package org.openmrs.module.mambaetl.reports.datim;

import org.openmrs.module.mambaetl.datasetdefinition.datim.HeaderDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.pmtct.PmtctArtDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.pmtct.PmtctHeiDataSetDefinitionMamba;
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
public class PMTCTEidReportsMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "8b561208-4052-4635-b345-71d5965b3e46";
	}
	
	@Override
	public String getName() {
		return "DATIM TREATMENT- PMTCT_EID";
	}
	
	@Override
	public String getDescription() {
		return "PMTCT_EID DATIM mamba report ";
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
		headerDefinition.setDescription("DSD: PMTCT_EID");
		headerDefinition.setParameters(getParameters());
		reportDefinition.addDataSetDefinition("DSD: PMTCT_EID", EthiOhriUtil.map(headerDefinition, "endDate=${endDateGC}"));
		
		PmtctHeiDataSetDefinitionMamba pmtctHeiDataSetDefinitionMamba = new PmtctHeiDataSetDefinitionMamba();
		pmtctHeiDataSetDefinitionMamba.addParameters(getParameters());
		pmtctHeiDataSetDefinitionMamba.setDescription("Disaggregated by infant age at test.");
		reportDefinition.addDataSetDefinition("Disaggregated by infant age at test.",
		    EthiOhriUtil.map(pmtctHeiDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("d4cf7744-6ad4-4b7f-9e9d-e0bbd2b09320", reportDefinition);
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
