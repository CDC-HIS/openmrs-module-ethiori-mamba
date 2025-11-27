package org.openmrs.module.mambaetl.reports.datim;

import org.openmrs.module.mambaetl.datasetdefinition.datim.HeaderDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.pmtct.PmtctHeiDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.mambaetl.helpers.reportOptions.HEIAggregationTypes;
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
public class PMTCTHeiReportsMamba implements ReportManager {

	@Override
	public String getUuid() {
		return "a5644908-8981-4904-973a-3eeaf6ccb234";
	}

	@Override
	public String getName() {
		return "DATIM TREATMENT- PMTCT_HEI";
	}

	@Override
	public String getDescription() {
		return "PMTCT_HEI DATIM mamba report ";
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
		headerDefinition.setDescription("DSD: PMTCT_HEI");
		headerDefinition.setParameters(getParameters());
		reportDefinition.addDataSetDefinition("DSD: PMTCT_HEI",
				EthiOhriUtil.map(headerDefinition, "endDate=${endDateGC}"));

		// Numerator: Auto-Calculate
		PmtctHeiDataSetDefinitionMamba numeratorDataSet = new PmtctHeiDataSetDefinitionMamba();
		numeratorDataSet.addParameters(getParameters());
		numeratorDataSet.setDescription("Auto-Calculate");
		numeratorDataSet.setReportType("NUMERATOR");
		reportDefinition.addDataSetDefinition("Numerator",
				EthiOhriUtil.map(numeratorDataSet, "startDate=${startDateGC},endDate=${endDateGC}"));

		// Disaggregated by infant age at virologic sample collection and result
		// returned
		PmtctHeiDataSetDefinitionMamba disaggregatedDataSet = new PmtctHeiDataSetDefinitionMamba();
		disaggregatedDataSet.addParameters(getParameters());
		disaggregatedDataSet.setHeiAggregationTypes(HEIAggregationTypes.RESULT_COLLECTED);
		disaggregatedDataSet
				.setDescription("Disaggregated by infant age at virologic sample collection and result returned.");
		disaggregatedDataSet.setReportType("DISAGGREGATED");
		reportDefinition.addDataSetDefinition("Infants who had a first virologic HIV test (sample collected) by:",
				EthiOhriUtil.map(disaggregatedDataSet, "startDate=${startDateGC},endDate=${endDateGC}"));

		return reportDefinition;
	}

	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {

		ReportDesign design = ReportManagerUtil.createExcelDesign("ded2a340-2b9c-4948-97c3-952d7b438aea",
				reportDefinition);
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
