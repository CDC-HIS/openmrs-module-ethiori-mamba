package org.openmrs.module.mambaetl.reports.datim;

import org.openmrs.module.mambaetl.datasetdefinition.datim.HeaderDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_tb.TxTBDenominatorDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_tb.TxTBNumeratorDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.mambaetl.helpers.reportOptions.TxTBAggregationTypes;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.ReportRequest;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.reporting.report.manager.ReportManager;
import org.openmrs.module.reporting.report.manager.ReportManagerUtil;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Component
public class TxTBNumeratorDATIMReportsMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "d5691326-eba3-4b93-a3ca-2d88f27d8303";
	}
	
	@Override
	public String getName() {
		return "MAMBA DATIM TREATMENT- TX_TB (Numerator)";
	}
	
	@Override
	public String getDescription() {
		return "TX curr TB DATIM mamba report (Numerator)";
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
		headerDefinition.setDescription("DSD: TX_TB (Numerator)");
		headerDefinition.setParameters(getParameters());
		reportDefinition.addDataSetDefinition("DSD: TX_TB (Numerator)",
		    EthiOhriUtil.map(headerDefinition, "endDate=${endDateGC}"));
		
		TxTBNumeratorDataSetDefinitionMamba txTBNewNumeratorTotalDataSetDefinitionMamba = new TxTBNumeratorDataSetDefinitionMamba();
		txTBNewNumeratorTotalDataSetDefinitionMamba.addParameters(getParameters());
		txTBNewNumeratorTotalDataSetDefinitionMamba.setTxTBAggregationTypes(TxTBAggregationTypes.NUMERATOR_TOTAL);
		txTBNewNumeratorTotalDataSetDefinitionMamba
		        .setDescription("Number of ART patients who were started on TB treatment during the reporting period. Numerator will auto-calculate from Already/New on ART by Age/Sex");
		reportDefinition
		        .addDataSetDefinition(
		            "Number of ART patients who were started on TB treatment during the reporting period. Numerator will auto-calculate from Already/New on ART by Age/Sex",
		            EthiOhriUtil.map(txTBNewNumeratorTotalDataSetDefinitionMamba,
		                "startDate=${startDateGC},endDate=${endDateGC}"));
		
		TxTBNumeratorDataSetDefinitionMamba txTBNewNumeratorDataSetDefinitionMamba = new TxTBNumeratorDataSetDefinitionMamba();
		txTBNewNumeratorDataSetDefinitionMamba.addParameters(getParameters());
		txTBNewNumeratorDataSetDefinitionMamba.setTxTBAggregationTypes(TxTBAggregationTypes.NUMERATOR_NEW);
		txTBNewNumeratorDataSetDefinitionMamba
		        .setDescription("The number of patients starting TB treatment who newly started ART during reporting period");
		reportDefinition.addDataSetDefinition(
		    "The number of patients starting TB treatment who newly started ART during reporting period",
		    EthiOhriUtil.map(txTBNewNumeratorDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		TxTBNumeratorDataSetDefinitionMamba txTBPrevNumeratorDataSetDefinitionMamba = new TxTBNumeratorDataSetDefinitionMamba();
		txTBPrevNumeratorDataSetDefinitionMamba.addParameters(getParameters());
		txTBPrevNumeratorDataSetDefinitionMamba.setTxTBAggregationTypes(TxTBAggregationTypes.NUMERATOR_NEW);
		txTBPrevNumeratorDataSetDefinitionMamba
		        .setDescription("The number of patients starting TB treatment who were already on ART prior to the start of the reporting period");
		reportDefinition
		        .addDataSetDefinition(
		            "The number of patients starting TB treatment who were already on ART prior to the start of the reporting period",
		            EthiOhriUtil.map(txTBPrevNumeratorDataSetDefinitionMamba,
		                "startDate=${startDateGC},endDate=${endDateGC}"));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		ReportDesign design = ReportManagerUtil.createExcelDesign("43109255-aa79-4722-a826-587bc48736b7", reportDefinition);
		
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
