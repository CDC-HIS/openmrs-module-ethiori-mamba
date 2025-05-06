package org.openmrs.module.mambaetl.reports.datim;

import org.openmrs.module.mambaetl.datasetdefinition.datim.HeaderDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_tb.TxTBDenominatorDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.mambaetl.helpers.reportOptions.TxTBAggregationTypes;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.ReportRequest;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.reporting.report.manager.ReportManager;
import org.openmrs.module.reporting.report.manager.ReportManagerUtil;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class TxTBDenominatorDATIMReportsMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "b7d4be9f-8a3d-4dc5-9b1c-7572f467c8e1";
	}
	
	@Override
	public String getName() {
		return "MAMBA DATIM TREATMENT- TX_TB (Denominator)";
	}
	
	@Override
	public String getDescription() {
		return "TX curr TB DATIM mamba report (Denominator)";
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
		headerDefinition.setDescription("DSD: TX_TB (Denominator)");
		headerDefinition.setParameters(getParameters());
		reportDefinition.addDataSetDefinition("DSD: TX_TB (Denominator)",
		    EthiOhriUtil.map(headerDefinition, "endDate=${endDateGC}"));
		
		TxTBDenominatorDataSetDefinitionMamba txTBNewArtPositiveDenominatorDataSetDefinitionMamba = new TxTBDenominatorDataSetDefinitionMamba();
		txTBNewArtPositiveDenominatorDataSetDefinitionMamba.addParameters(getParameters());
		txTBNewArtPositiveDenominatorDataSetDefinitionMamba.setTxTBAggregationTypes(TxTBAggregationTypes.NEW_ART_POSITIVE);
		txTBNewArtPositiveDenominatorDataSetDefinitionMamba.setDescription("New on ART/Screen Positive");
		reportDefinition.addDataSetDefinition("New on ART/Screen Positive", EthiOhriUtil.map(
		    txTBNewArtPositiveDenominatorDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		TxTBDenominatorDataSetDefinitionMamba txTBNewArtNegativeDenominatorDataSetDefinitionMamba = new TxTBDenominatorDataSetDefinitionMamba();
		txTBNewArtNegativeDenominatorDataSetDefinitionMamba.addParameters(getParameters());
		txTBNewArtNegativeDenominatorDataSetDefinitionMamba.setTxTBAggregationTypes(TxTBAggregationTypes.NEW_ART_NEGATIVE);
		txTBNewArtNegativeDenominatorDataSetDefinitionMamba.setDescription("New on ART/Screen Negative");
		reportDefinition.addDataSetDefinition("New on ART/Screen Negative", EthiOhriUtil.map(
		    txTBNewArtNegativeDenominatorDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		TxTBDenominatorDataSetDefinitionMamba txTBPreArtPositiveDenominatorDataSetDefinitionMamba = new TxTBDenominatorDataSetDefinitionMamba();
		txTBPreArtPositiveDenominatorDataSetDefinitionMamba.addParameters(getParameters());
		txTBPreArtPositiveDenominatorDataSetDefinitionMamba.setTxTBAggregationTypes(TxTBAggregationTypes.PREV_ART_POSITIVE);
		txTBPreArtPositiveDenominatorDataSetDefinitionMamba.setDescription("Already on ART/Screen Positive");
		reportDefinition.addDataSetDefinition("Already on ART/Screen Positive", EthiOhriUtil.map(
		    txTBPreArtPositiveDenominatorDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		TxTBDenominatorDataSetDefinitionMamba txTBPreArtNegativeDenominatorDataSetDefinitionMamba = new TxTBDenominatorDataSetDefinitionMamba();
		txTBPreArtNegativeDenominatorDataSetDefinitionMamba.addParameters(getParameters());
		txTBPreArtNegativeDenominatorDataSetDefinitionMamba.setTxTBAggregationTypes(TxTBAggregationTypes.PREV_ART_NEGATIVE);
		txTBPreArtNegativeDenominatorDataSetDefinitionMamba.setDescription("Already on ART/Screen Negative");
		reportDefinition.addDataSetDefinition("Already on ART/Screen Negative", EthiOhriUtil.map(
		    txTBPreArtNegativeDenominatorDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		TxTBDenominatorDataSetDefinitionMamba txTBScreenTypeDenominatorDataSetDefinitionMamba = new TxTBDenominatorDataSetDefinitionMamba();
		txTBScreenTypeDenominatorDataSetDefinitionMamba.addParameters(getParameters());
		txTBScreenTypeDenominatorDataSetDefinitionMamba.setTxTBAggregationTypes(TxTBAggregationTypes.PREV_ART_NEGATIVE);
		txTBScreenTypeDenominatorDataSetDefinitionMamba.setDescription("Disaggregated by Screen Type");
		reportDefinition.addDataSetDefinition("Disaggregated by Screen Type", EthiOhriUtil.map(
		    txTBScreenTypeDenominatorDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		TxTBDenominatorDataSetDefinitionMamba txTBSpecimenSentDenominatorDataSetDefinitionMamba = new TxTBDenominatorDataSetDefinitionMamba();
		txTBSpecimenSentDenominatorDataSetDefinitionMamba.addParameters(getParameters());
		txTBSpecimenSentDenominatorDataSetDefinitionMamba.setTxTBAggregationTypes(TxTBAggregationTypes.SPECIMEN_SENT);
		txTBSpecimenSentDenominatorDataSetDefinitionMamba.setDescription("Disaggregated by Specimen Sent");
		reportDefinition.addDataSetDefinition("Disaggregated by Specimen Sent", EthiOhriUtil.map(
		    txTBSpecimenSentDenominatorDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		TxTBDenominatorDataSetDefinitionMamba txTBDiagnosticTestDenominatorDataSetDefinitionMamba = new TxTBDenominatorDataSetDefinitionMamba();
		txTBDiagnosticTestDenominatorDataSetDefinitionMamba.addParameters(getParameters());
		txTBDiagnosticTestDenominatorDataSetDefinitionMamba.setTxTBAggregationTypes(TxTBAggregationTypes.DIAGNOSTIC_TEST);
		txTBDiagnosticTestDenominatorDataSetDefinitionMamba.setDescription("[Disagg of Specimen Sent] Diagnostic Test");
		reportDefinition.addDataSetDefinition("[Disagg of Specimen Sent] Diagnostic Test", EthiOhriUtil.map(
		    txTBDiagnosticTestDenominatorDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		TxTBDenominatorDataSetDefinitionMamba txTBPositiveReturnedDenominatorDataSetDefinitionMamba = new TxTBDenominatorDataSetDefinitionMamba();
		txTBPositiveReturnedDenominatorDataSetDefinitionMamba.addParameters(getParameters());
		txTBPositiveReturnedDenominatorDataSetDefinitionMamba.setTxTBAggregationTypes(TxTBAggregationTypes.POSITIVE_RESULT);
		txTBPositiveReturnedDenominatorDataSetDefinitionMamba.setDescription("Disaggregated by Positive Result Returned");
		reportDefinition.addDataSetDefinition("Disaggregated by Positive Result Returned", EthiOhriUtil.map(
		    txTBPositiveReturnedDenominatorDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		ReportDesign design = ReportManagerUtil.createExcelDesign("b395cbcb-79a1-44ff-a1c9-a7f8ed254c8f", reportDefinition);
		
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
