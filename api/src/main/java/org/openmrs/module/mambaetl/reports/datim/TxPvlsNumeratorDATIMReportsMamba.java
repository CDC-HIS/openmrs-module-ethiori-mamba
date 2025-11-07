package org.openmrs.module.mambaetl.reports.datim;

import org.openmrs.module.mambaetl.datasetdefinition.datim.KeyPopulationDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_curr_pvls.TxCurrPvlsDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.HeaderDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.mambaetl.helpers.reportOptions.TxCurrPvlsAggregationTypes;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.ReportRequest;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.reporting.report.manager.ReportManager;
import org.openmrs.module.reporting.report.manager.ReportManagerUtil;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class TxPvlsNumeratorDATIMReportsMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "0fcca403-070b-4012-8397-07067ed0f320";
	}
	
	@Override
	public String getName() {
		return "DATIM VIRAL SUPPRESSION- TX_PVLS (Numerator) ";
	}
	
	@Override
	public String getDescription() {
		return "TX curr PVLS DATIM mamba report (Numerator) ";
	}
	
	@Override
	public List<Parameter> getParameters() {
		
		return EthiOhriUtil.getEndDateParameters(Boolean.TRUE);
		
	}
	
	@Override
	public ReportDefinition constructReportDefinition() {
		ReportDefinition reportDefinition = new ReportDefinition();
		reportDefinition.setUuid(getUuid());
		reportDefinition.setName(getName());
		reportDefinition.setDescription(getDescription());
		reportDefinition.setParameters(getParameters());
		
		HeaderDataSetDefinitionMamba headerDefinition = new HeaderDataSetDefinitionMamba();
		headerDefinition.setDescription("DSD: TX_PVLS (Numerator)");
		headerDefinition.setParameters(getParameters());
		reportDefinition.addDataSetDefinition("DSD: TX_PVLS (Numerator)",
		    EthiOhriUtil.map(headerDefinition, "endDate=${endDateGC}"));
		
		TxCurrPvlsDataSetDefinitionMamba txCurrPvlsDataSetDefinitionNumeratorTotalMamba = new TxCurrPvlsDataSetDefinitionMamba();
		txCurrPvlsDataSetDefinitionNumeratorTotalMamba.addParameters(getParameters());
		txCurrPvlsDataSetDefinitionNumeratorTotalMamba
		        .setTxCurrPvlsAggregationTypes(TxCurrPvlsAggregationTypes.NUMERATOR_TOTAL);
		txCurrPvlsDataSetDefinitionNumeratorTotalMamba.setDescription("Numerator");
		reportDefinition.addDataSetDefinition("Numerator",
		    EthiOhriUtil.map(txCurrPvlsDataSetDefinitionNumeratorTotalMamba, "endDate=${endDateGC}"));
		
		TxCurrPvlsDataSetDefinitionMamba txCurrPvlsDataSetDefinitionNumeratorMamba = new TxCurrPvlsDataSetDefinitionMamba();
		txCurrPvlsDataSetDefinitionNumeratorMamba.addParameters(getParameters());
		txCurrPvlsDataSetDefinitionNumeratorMamba.setTxCurrPvlsAggregationTypes(TxCurrPvlsAggregationTypes.NUMERATOR);
		txCurrPvlsDataSetDefinitionNumeratorMamba
		        .setDescription("Disaggregated by Age / Sex (Fine Disaggregate). Must complete finer disaggregates unless permitted by program.");
		reportDefinition
		        .addDataSetDefinition(
		            "Disaggregated by Age / Sex (Fine Disaggregate). Must complete finer disaggregates unless permitted by program.",
		            EthiOhriUtil.map(txCurrPvlsDataSetDefinitionNumeratorMamba, "endDate=${endDateGC}"));
		
		TxCurrPvlsDataSetDefinitionMamba txCurrPvlsDataSetDefinitionNumeratorBreastFeedingPregnantMamba = new TxCurrPvlsDataSetDefinitionMamba();
		txCurrPvlsDataSetDefinitionNumeratorBreastFeedingPregnantMamba.addParameters(getParameters());
		txCurrPvlsDataSetDefinitionNumeratorBreastFeedingPregnantMamba
		        .setTxCurrPvlsAggregationTypes(TxCurrPvlsAggregationTypes.NUMERATOR_BREAST_FEEDING_PREGNANT);
		txCurrPvlsDataSetDefinitionNumeratorBreastFeedingPregnantMamba
		        .setDescription("Disaggregated by Pregnant/Breastfeeding.");
		reportDefinition.addDataSetDefinition("Disaggregated by Pregnant/Breastfeeding.",
		    EthiOhriUtil.map(txCurrPvlsDataSetDefinitionNumeratorBreastFeedingPregnantMamba, "endDate=${endDateGC}"));
		
//		KeyPopulationDataSetDefinitionMamba keyPopulationDataSetDefinitionMamba = new KeyPopulationDataSetDefinitionMamba();
//		keyPopulationDataSetDefinitionMamba.addParameters(getParameters());
//		reportDefinition.addDataSetDefinition("Disaggregated by key population type",
//		    EthiOhriUtil.map(keyPopulationDataSetDefinitionMamba, "endDate=${endDateGC}"));
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		ReportDesign design = ReportManagerUtil.createExcelDesign("3e308a1b-77e7-4924-8373-656e544e31d8", reportDefinition);
		design.setReportDefinition(reportDefinition);
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
