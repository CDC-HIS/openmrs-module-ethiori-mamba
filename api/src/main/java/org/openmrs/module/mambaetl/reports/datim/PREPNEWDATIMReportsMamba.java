package org.openmrs.module.mambaetl.reports.datim;

import org.openmrs.module.mambaetl.datasetdefinition.datim.HeaderDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.prep_new.PrEPNEWDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_ml.MLKeyPopulationDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.mambaetl.helpers.reportOptions.PrEPNEWAggregationTypes;
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
public class PREPNEWDATIMReportsMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "c4a21a0b-4d68-48b9-81ab-22d7477a2ed9";
	}
	
	@Override
	public String getName() {
		return "DATIM PREVENTION- PrEP_NEW";
	}
	
	@Override
	public String getDescription() {
		return "PrEP_NEW DATIM mamba report";
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
		headerDefinition.setDescription("DSD: PrEP_NEW");
		headerDefinition.setParameters(getParameters());
		reportDefinition.addDataSetDefinition("DSD: PrEP_NEW", EthiOhriUtil.map(headerDefinition, "endDate=${endDateGC}"));
		
		PrEPNEWDataSetDefinitionMamba prepNewTotalDatimReportsMamba = new PrEPNEWDataSetDefinitionMamba();
		prepNewTotalDatimReportsMamba.addParameters(getParameters());
		prepNewTotalDatimReportsMamba.setPrEPNEWAggregationTypes(PrEPNEWAggregationTypes.TOTAL);
		prepNewTotalDatimReportsMamba
		        .setDescription("Number of individuals who were newly enrolled on pre-exposure prophylaxis (PrEP) to prevent HIV infection in\n"
		                + "the reporting period. Numerator will auto-calculate from the Age/Sex Disaggregates.");
		reportDefinition.addDataSetDefinition(
		    "Number of individuals who were newly enrolled on pre-exposure prophylaxis (PrEP) to prevent HIV infection in\n"
		            + "the reporting period. Numerator will auto-calculate from the Age/Sex Disaggregates.",
		    EthiOhriUtil.map(prepNewTotalDatimReportsMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		PrEPNEWDataSetDefinitionMamba prepctAgeSexdatimReportsMamba = new PrEPNEWDataSetDefinitionMamba();
		prepctAgeSexdatimReportsMamba.addParameters(getParameters());
		prepctAgeSexdatimReportsMamba.setPrEPNEWAggregationTypes(PrEPNEWAggregationTypes.AGE_SEX);
		prepctAgeSexdatimReportsMamba.setDescription("Disaggregated by Age/Sex");
		reportDefinition.addDataSetDefinition("Disaggregated by Age/Sex",
		    EthiOhriUtil.map(prepctAgeSexdatimReportsMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		MLKeyPopulationDataSetDefinitionMamba mlKeyPopulationDataSetDefinitionMamba = new MLKeyPopulationDataSetDefinitionMamba();
		mlKeyPopulationDataSetDefinitionMamba.addParameters(getParameters());
		mlKeyPopulationDataSetDefinitionMamba.setDescription("Disaggregated by Status/Key Population Type:");
		reportDefinition.addDataSetDefinition("Disaggregated by Status/Key Population Type:",
		    EthiOhriUtil.map(mlKeyPopulationDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		PrEPNEWDataSetDefinitionMamba prEPCTBreastFeedingDataSetDefinitionMamba = new PrEPNEWDataSetDefinitionMamba();
		prEPCTBreastFeedingDataSetDefinitionMamba.addParameters(getParameters());
		prEPCTBreastFeedingDataSetDefinitionMamba.setPrEPNEWAggregationTypes(PrEPNEWAggregationTypes.PREGNANT_BF);
		prEPCTBreastFeedingDataSetDefinitionMamba.setDescription("Disaggregated by Pregnant/Breastfeeding Status:");
		reportDefinition.addDataSetDefinition("Disaggregated by Pregnant/Breastfeeding Status:",
		    EthiOhriUtil.map(prEPCTBreastFeedingDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		PrEPNEWDataSetDefinitionMamba prEPCTFacilityDataSetDefinitionMamba = new PrEPNEWDataSetDefinitionMamba();
		prEPCTFacilityDataSetDefinitionMamba.addParameters(getParameters());
		prEPCTFacilityDataSetDefinitionMamba.setPrEPNEWAggregationTypes(PrEPNEWAggregationTypes.FACILITY);
		prEPCTFacilityDataSetDefinitionMamba.setDescription("Disaggregated by PrEP Distribution.");
		reportDefinition.addDataSetDefinition("Disaggregated by PrEP Distribution.",
		    EthiOhriUtil.map(prEPCTFacilityDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		PrEPNEWDataSetDefinitionMamba prEPCTPrepTypeDataSetDefinitionMamba = new PrEPNEWDataSetDefinitionMamba();
		prEPCTPrepTypeDataSetDefinitionMamba.addParameters(getParameters());
		prEPCTPrepTypeDataSetDefinitionMamba.setPrEPNEWAggregationTypes(PrEPNEWAggregationTypes.PREP_TYPE);
		prEPCTPrepTypeDataSetDefinitionMamba.setDescription("Disaggregated by PrEP Type.");
		reportDefinition.addDataSetDefinition("Disaggregated by PrEP Type.",
		    EthiOhriUtil.map(prEPCTPrepTypeDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("484c8911-ecc4-4756-a228-4ea46ddebdc7", reportDefinition);
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
