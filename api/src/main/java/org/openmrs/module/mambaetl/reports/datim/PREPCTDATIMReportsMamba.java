package org.openmrs.module.mambaetl.reports.datim;

import org.openmrs.module.mambaetl.datasetdefinition.datim.HeaderDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.prep_ct.PrEPCTDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_ml.MLKeyPopulationDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.mambaetl.helpers.reportOptions.PrEPCTggregationTypes;
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
public class PREPCTDATIMReportsMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "6eaa63c0-4a4d-4cb8-93d1-9fa516ccfca0";
	}
	
	@Override
	public String getName() {
		return "MAMBA DATIM PREVENTION- PrEP_CT";
	}
	
	@Override
	public String getDescription() {
		return "PrEP_CT DATIM mamba report";
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
		
		HeaderDataSetDefinitionMamba headerDefinition = new HeaderDataSetDefinitionMamba();
		headerDefinition.setDescription("DSD: PrEP_CT");
		headerDefinition.setParameters(getParameters());
		reportDefinition.addDataSetDefinition("DSD: PrEP_CT", EthiOhriUtil.map(headerDefinition, "endDate=${endDateGC}"));
		
		PrEPCTDataSetDefinitionMamba prepctTotaldatimReportsMamba = new PrEPCTDataSetDefinitionMamba();
		prepctTotaldatimReportsMamba.addParameters(getParameters());
		prepctTotaldatimReportsMamba.setPrEPCTggregationTypes(PrEPCTggregationTypes.TOTAL);
		prepctTotaldatimReportsMamba
		        .setDescription("Number of individuals, excluding those newly enrolled, that return for a follow-up visit or reinitiation visit to\n"
		                + "receive pre-exposure prophylaxis (PrEP) to prevent HIV during the reporting period. Numerator will auto-\n"
		                + "calculate from sum of Age/Sex Disaggregate");
		reportDefinition
		        .addDataSetDefinition(
		            "Number of individuals, excluding those newly enrolled, that return for a follow-up visit or reinitiation visit to\n"
		                    + "receive pre-exposure prophylaxis (PrEP) to prevent HIV during the reporting period. Numerator will auto-\n"
		                    + "calculate from sum of Age/Sex Disaggregate",
		            EthiOhriUtil.map(prepctTotaldatimReportsMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		PrEPCTDataSetDefinitionMamba prepctAgeSexdatimReportsMamba = new PrEPCTDataSetDefinitionMamba();
		prepctAgeSexdatimReportsMamba.addParameters(getParameters());
		prepctAgeSexdatimReportsMamba.setPrEPCTggregationTypes(PrEPCTggregationTypes.AGE_SEX);
		prepctAgeSexdatimReportsMamba.setDescription("Disaggregated by Age/Sex");
		reportDefinition.addDataSetDefinition("Disaggregated by Age/Sex",
		    EthiOhriUtil.map(prepctAgeSexdatimReportsMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		PrEPCTDataSetDefinitionMamba prepctTestResultdatimReportsMamba = new PrEPCTDataSetDefinitionMamba();
		prepctTestResultdatimReportsMamba.addParameters(getParameters());
		prepctTestResultdatimReportsMamba.setPrEPCTggregationTypes(PrEPCTggregationTypes.TEST_RESULT);
		prepctTestResultdatimReportsMamba.setDescription("Disaggregated by test result");
		reportDefinition.addDataSetDefinition("Disaggregated by test result",
		    EthiOhriUtil.map(prepctTestResultdatimReportsMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		MLKeyPopulationDataSetDefinitionMamba mlKeyPopulationDataSetDefinitionMamba = new MLKeyPopulationDataSetDefinitionMamba();
		mlKeyPopulationDataSetDefinitionMamba.addParameters(getParameters());
		mlKeyPopulationDataSetDefinitionMamba.setDescription("Disaggregated by Status/Key Population Type:");
		reportDefinition.addDataSetDefinition("Disaggregated by Status/Key Population Type:",
		    EthiOhriUtil.map(mlKeyPopulationDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		PrEPCTDataSetDefinitionMamba prEPCTBreastFeedingDataSetDefinitionMamba = new PrEPCTDataSetDefinitionMamba();
		prEPCTBreastFeedingDataSetDefinitionMamba.addParameters(getParameters());
		prEPCTBreastFeedingDataSetDefinitionMamba.setPrEPCTggregationTypes(PrEPCTggregationTypes.PREGNANT_BF);
		prEPCTBreastFeedingDataSetDefinitionMamba.setDescription("Disaggregated by Pregnant/Breastfeeding Status:");
		reportDefinition.addDataSetDefinition("Disaggregated by Pregnant/Breastfeeding Status:",
		    EthiOhriUtil.map(prEPCTBreastFeedingDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		PrEPCTDataSetDefinitionMamba prEPCTFacilityDataSetDefinitionMamba = new PrEPCTDataSetDefinitionMamba();
		prEPCTFacilityDataSetDefinitionMamba.addParameters(getParameters());
		prEPCTFacilityDataSetDefinitionMamba.setPrEPCTggregationTypes(PrEPCTggregationTypes.FACILITY);
		prEPCTFacilityDataSetDefinitionMamba.setDescription("Disaggregated by PrEP Distribution.");
		reportDefinition.addDataSetDefinition("Disaggregated by PrEP Distribution.",
		    EthiOhriUtil.map(prEPCTFacilityDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		PrEPCTDataSetDefinitionMamba prEPCTPrepTypeDataSetDefinitionMamba = new PrEPCTDataSetDefinitionMamba();
		prEPCTPrepTypeDataSetDefinitionMamba.addParameters(getParameters());
		prEPCTPrepTypeDataSetDefinitionMamba.setPrEPCTggregationTypes(PrEPCTggregationTypes.PREP_TYPE);
		prEPCTPrepTypeDataSetDefinitionMamba.setDescription("Disaggregated by PrEP Type.");
		reportDefinition.addDataSetDefinition("Disaggregated by PrEP Type.",
		    EthiOhriUtil.map(prEPCTPrepTypeDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("26c67b6e-124e-4f9a-99d6-06d79f904b26", reportDefinition);
		
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
