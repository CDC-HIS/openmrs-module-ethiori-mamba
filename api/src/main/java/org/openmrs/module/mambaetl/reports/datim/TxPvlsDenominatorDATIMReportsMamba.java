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
public class TxPvlsDenominatorDATIMReportsMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "a2fa1ba3-4af2-41b4-90a3-c0d6be2fa24f";
	} //4d7b385f-331f-400c-8592-f539f4565d9d
	
	@Override
	public String getName() {
		return "MAMBA DATIM VIRAL SUPPRESSION- TX_PVLS (Denominator)";
	}
	
	@Override
	public String getDescription() {
		return "TX curr PVLS DATIM mamba report (Denominator)";
	}
	
	@Override
	public List<Parameter> getParameters() {
		
		Parameter endDate = new Parameter("endDate", "On Month", Date.class);
		endDate.setRequired(false);
		Parameter endDateGC = new Parameter("endDateGC", " ", Date.class);
		endDateGC.setRequired(false);
		return Arrays.asList(endDate, endDateGC);
		
	}
	
	@Override
	public ReportDefinition constructReportDefinition() {
		ReportDefinition reportDefinition = new ReportDefinition();
		reportDefinition.setUuid(getUuid());
		reportDefinition.setName(getName());
		reportDefinition.setDescription(getDescription());
		reportDefinition.setParameters(getParameters());
		
		HeaderDataSetDefinitionMamba headerDefinition = new HeaderDataSetDefinitionMamba();
		headerDefinition.setDescription("DSD: TX_PVLS (Denominator)");
		headerDefinition.setParameters(getParameters());
		reportDefinition.addDataSetDefinition("DSD: TX_PVLS (Denominator)",
		    EthiOhriUtil.map(headerDefinition, "endDate=${endDateGC}"));
		
		TxCurrPvlsDataSetDefinitionMamba txCurrPvlsDataSetDefinitionDenominatorTotalMamba = new TxCurrPvlsDataSetDefinitionMamba();
		txCurrPvlsDataSetDefinitionDenominatorTotalMamba.addParameters(getParameters());
		txCurrPvlsDataSetDefinitionDenominatorTotalMamba
		        .setTxCurrPvlsAggregationTypes(TxCurrPvlsAggregationTypes.DENOMINATOR_TOTAL);
		txCurrPvlsDataSetDefinitionDenominatorTotalMamba.setDescription("Denominator");
		reportDefinition.addDataSetDefinition("Denominator",
		    EthiOhriUtil.map(txCurrPvlsDataSetDefinitionDenominatorTotalMamba, "endDate=${endDateGC}"));
		
		TxCurrPvlsDataSetDefinitionMamba txCurrPvlsDataSetDefinitionDenominatorMamba = new TxCurrPvlsDataSetDefinitionMamba();
		txCurrPvlsDataSetDefinitionDenominatorMamba.addParameters(getParameters());
		txCurrPvlsDataSetDefinitionDenominatorMamba.setTxCurrPvlsAggregationTypes(TxCurrPvlsAggregationTypes.DENOMINATOR);
		txCurrPvlsDataSetDefinitionDenominatorMamba
		        .setDescription("Disaggregated by Age / Sex (Fine Disaggregate). Must complete finer disaggregates unless permitted by program.");
		reportDefinition
		        .addDataSetDefinition(
		            "Disaggregated by Age / Sex (Fine Disaggregate). Must complete finer disaggregates unless permitted by program.",
		            EthiOhriUtil.map(txCurrPvlsDataSetDefinitionDenominatorMamba, "endDate=${endDateGC}"));
		
		TxCurrPvlsDataSetDefinitionMamba txCurrPvlsDataSetDefinitionDenominatorBreastFeedingPregnantMamba = new TxCurrPvlsDataSetDefinitionMamba();
		txCurrPvlsDataSetDefinitionDenominatorBreastFeedingPregnantMamba.addParameters(getParameters());
		txCurrPvlsDataSetDefinitionDenominatorBreastFeedingPregnantMamba
		        .setTxCurrPvlsAggregationTypes(TxCurrPvlsAggregationTypes.DENOMINATOR_BREAST_FEEDING_PREGNANT);
		txCurrPvlsDataSetDefinitionDenominatorBreastFeedingPregnantMamba
		        .setDescription("Disaggregated by Pregnant/Breastfeeding.");
		reportDefinition.addDataSetDefinition("Disaggregated by Pregnant/Breastfeeding.",
		    EthiOhriUtil.map(txCurrPvlsDataSetDefinitionDenominatorBreastFeedingPregnantMamba, "endDate=${endDateGC}"));
		
		KeyPopulationDataSetDefinitionMamba keyPopulationDataSetDefinitionMamba = new KeyPopulationDataSetDefinitionMamba();
		keyPopulationDataSetDefinitionMamba.addParameters(getParameters());
		reportDefinition.addDataSetDefinition("Disaggregated by key population type",
		    EthiOhriUtil.map(keyPopulationDataSetDefinitionMamba, "endDate=${endDateGC}"));
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		ReportDesign design = ReportManagerUtil.createExcelDesign("cd2c6cc1-4a7e-482f-8ece-710914eb072c", reportDefinition);
		
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
