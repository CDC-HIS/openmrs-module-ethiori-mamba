package org.openmrs.module.mambaetl.reports.datim;

import org.openmrs.module.mambaetl.datasetdefinition.datim.HeaderDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.KeyPopulationDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_rtt.TxRTTDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.mambaetl.helpers.reportOptions.TxRTTAggregationTypes;
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
public class TXRTTDATIMReportsMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "21b8a53d-a5d7-4e20-ab23-bc4c81ff9d02";
	}
	
	@Override
	public String getName() {
		return "MAMBA DATIM TREATMENT- TX_RTT";
	}
	
	@Override
	public String getDescription() {
		return "TX_RTT DATIM mamba report";
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
		headerDefinition.setDescription("DSD: TX_RTT");
		headerDefinition.setParameters(getParameters());
		reportDefinition.addDataSetDefinition("DSD: TX_RTT", EthiOhriUtil.map(headerDefinition, "endDate=${endDateGC}"));
		
		TxRTTDataSetDefinitionMamba txRTTTotalDataSetDefinitionMamba = new TxRTTDataSetDefinitionMamba();
		txRTTTotalDataSetDefinitionMamba.addParameters(getParameters());
		txRTTTotalDataSetDefinitionMamba.setTxRTTAggregationTypes(TxRTTAggregationTypes.TOTAL);
		txRTTTotalDataSetDefinitionMamba
		        .setDescription("Number of ART patients who experienced an interruption in treatment (IIT) during any previous reporting\n"
		                + "period, who successfully restarted ARVs within the reporting period and remained on treatment until the end of\n"
		                + "the reporting period. The Numerator will auto-calculate from Age/Sex Disaggregates.");
		reportDefinition
		        .addDataSetDefinition(
		            "Number of ART patients who experienced an interruption in treatment (IIT) during any previous reporting\n"
		                    + "period, who successfully restarted ARVs within the reporting period and remained on treatment until the end of\n"
		                    + "the reporting period. The Numerator will auto-calculate from Age/Sex Disaggregates.",
		            EthiOhriUtil.map(txRTTTotalDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		HeaderDataSetDefinitionMamba header2Definition = new HeaderDataSetDefinitionMamba();
		header2Definition.setDescription("Disaggregated by Age/Sex and CD4");
		header2Definition.setParameters(getParameters());
		reportDefinition.addDataSetDefinition("Disaggregated by Age/Sex and CD4",
		    EthiOhriUtil.map(header2Definition, "endDate=${endDateGC}"));
		
		TxRTTDataSetDefinitionMamba txRTTDataSetDefinitionMamba = new TxRTTDataSetDefinitionMamba();
		txRTTDataSetDefinitionMamba.addParameters(getParameters());
		txRTTDataSetDefinitionMamba.setTxRTTAggregationTypes(TxRTTAggregationTypes.CD4_LESS_THAN_200);
		txRTTDataSetDefinitionMamba.setDescription("<200 CD4");
		reportDefinition.addDataSetDefinition("<200 CD4",
		    EthiOhriUtil.map(txRTTDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		TxRTTDataSetDefinitionMamba txRTTGreaterThan200DataSetDefinitionMamba = new TxRTTDataSetDefinitionMamba();
		txRTTGreaterThan200DataSetDefinitionMamba.addParameters(getParameters());
		txRTTGreaterThan200DataSetDefinitionMamba.setTxRTTAggregationTypes(TxRTTAggregationTypes.CD4_GREATER_THAN_200);
		txRTTGreaterThan200DataSetDefinitionMamba.setDescription("≥200 CD4");
		reportDefinition.addDataSetDefinition("≥200 CD4",
		    EthiOhriUtil.map(txRTTGreaterThan200DataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		TxRTTDataSetDefinitionMamba txRTTUnknownDataSetDefinitionMamba = new TxRTTDataSetDefinitionMamba();
		txRTTUnknownDataSetDefinitionMamba.addParameters(getParameters());
		txRTTUnknownDataSetDefinitionMamba.setTxRTTAggregationTypes(TxRTTAggregationTypes.CD4_UNKNOWN);
		txRTTUnknownDataSetDefinitionMamba.setDescription("Unknown CD4");
		reportDefinition.addDataSetDefinition("Unknown CD4",
		    EthiOhriUtil.map(txRTTUnknownDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		TxRTTDataSetDefinitionMamba txRTTNotEligibleDataSetDefinitionMamba = new TxRTTDataSetDefinitionMamba();
		txRTTNotEligibleDataSetDefinitionMamba.addParameters(getParameters());
		txRTTNotEligibleDataSetDefinitionMamba.setTxRTTAggregationTypes(TxRTTAggregationTypes.CD4_NOT_ELIGIBLE);
		txRTTNotEligibleDataSetDefinitionMamba.setDescription("Not Eligible for CD4");
		reportDefinition.addDataSetDefinition("Not Eligible for CD4",
		    EthiOhriUtil.map(txRTTNotEligibleDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		TxRTTDataSetDefinitionMamba txRTTIITDataSetDefinitionMamba = new TxRTTDataSetDefinitionMamba();
		txRTTIITDataSetDefinitionMamba.addParameters(getParameters());
		txRTTIITDataSetDefinitionMamba.setTxRTTAggregationTypes(TxRTTAggregationTypes.IIT);
		txRTTIITDataSetDefinitionMamba.setDescription("Disaggregated by IIT");
		reportDefinition.addDataSetDefinition("Disaggregated by IIT",
		    EthiOhriUtil.map(txRTTIITDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		KeyPopulationDataSetDefinitionMamba keyPopulationDataSetDefinitionMamba = new KeyPopulationDataSetDefinitionMamba();
		keyPopulationDataSetDefinitionMamba.addParameters(getParameters());
		reportDefinition.addDataSetDefinition("Disaggregated by key population type",
		    EthiOhriUtil.map(keyPopulationDataSetDefinitionMamba, "endDate=${endDateGC}"));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		ReportDesign design = ReportManagerUtil.createExcelDesign("9a5a07af-c223-46d6-b2f1-5b2f7e2074ad", reportDefinition);
		
		return Collections.singletonList(design);
	}
	
	@Override
	public List<ReportRequest> constructScheduledRequests(ReportDefinition reportDefinition) {
		return Collections.emptyList();
	}
	
	@Override
	public String getVersion() {
		return "1.0.0-SNAPSHOT";
	}
}
