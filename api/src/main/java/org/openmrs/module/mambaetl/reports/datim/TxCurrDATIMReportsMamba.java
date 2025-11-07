package org.openmrs.module.mambaetl.reports.datim;

import org.openmrs.module.mambaetl.datasetdefinition.datim.KeyPopulationDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_curr.TxCurrAgeSexDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.HeaderDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.mambaetl.helpers.reportOptions.TxCurrAggregationTypes;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.ReportRequest;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.reporting.report.manager.ReportManager;
import org.openmrs.module.reporting.report.manager.ReportManagerUtil;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class TxCurrDATIMReportsMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "d4debfa2-95ea-4c8e-9a89-36ccd201e943";
	}
	
	@Override
	public String getName() {
		return "DATIM TREATMENT- TX_CURR";
	}
	
	@Override
	public String getDescription() {
		return "TX curr DATIM mamba report ";
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
		headerDefinition.setDescription("DSD: TX_CURR");
		headerDefinition.setParameters(getParameters());
		reportDefinition.addDataSetDefinition("DSD: TX_CURR", EthiOhriUtil.map(headerDefinition, "endDate=${endDateGC}"));
		
		TxCurrAgeSexDataSetDefinitionMamba txCurrAgeSexNumeratorDataSetDefinitionMamba = new TxCurrAgeSexDataSetDefinitionMamba();
		txCurrAgeSexNumeratorDataSetDefinitionMamba.addParameters(getParameters());
		txCurrAgeSexNumeratorDataSetDefinitionMamba.setTxCurrAggregationType(TxCurrAggregationTypes.NUMERATOR);
		txCurrAgeSexNumeratorDataSetDefinitionMamba
		        .setDescription("Number of adults and children currently receiving antiretroviral therapy (ART). Numerator will auto-calculate from Age/Sex Disaggregates.");
		reportDefinition
		        .addDataSetDefinition(
		            "Number of adults and children currently receiving antiretroviral therapy (ART). Numerator will auto-calculate from Age/Sex Disaggregates.",
		            EthiOhriUtil.map(txCurrAgeSexNumeratorDataSetDefinitionMamba, "endDate=${endDateGC}"));
		
		TxCurrAgeSexDataSetDefinitionMamba txCurrAgeSexDataSetDefinitionMamba = new TxCurrAgeSexDataSetDefinitionMamba();
		txCurrAgeSexDataSetDefinitionMamba.addParameters(getParameters());
		txCurrAgeSexDataSetDefinitionMamba.setTxCurrAggregationType(TxCurrAggregationTypes.AGE_SEX);
		txCurrAgeSexDataSetDefinitionMamba.setDescription("Disaggregated by Age / Sex (Fine Disaggregate)");
		reportDefinition.addDataSetDefinition("Disaggregated by Age / Sex (Fine Disaggregate)",
		    EthiOhriUtil.map(txCurrAgeSexDataSetDefinitionMamba, "endDate=${endDateGC}"));
		
		TxCurrAgeSexDataSetDefinitionMamba txCurrCd4DataSetDefinitionMamba = new TxCurrAgeSexDataSetDefinitionMamba();
		txCurrCd4DataSetDefinitionMamba.addParameters(getParameters());
		txCurrCd4DataSetDefinitionMamba.setTxCurrAggregationType(TxCurrAggregationTypes.CD4);
		txCurrCd4DataSetDefinitionMamba.setDescription("Disaggregated by ARV Dispensing Quantity by Coarse Age/Sex");
		reportDefinition.addDataSetDefinition("Disaggregated by ARV Dispensing Quantity by Coarse Age/Sex)",
		    EthiOhriUtil.map(txCurrCd4DataSetDefinitionMamba, "endDate=${endDateGC}"));
		
//		KeyPopulationDataSetDefinitionMamba keyPopulationDataSetDefinitionMamba = new KeyPopulationDataSetDefinitionMamba();
//		keyPopulationDataSetDefinitionMamba.addParameters(getParameters());
//		reportDefinition.addDataSetDefinition("Disaggregated by key population type",
//		    EthiOhriUtil.map(keyPopulationDataSetDefinitionMamba, "endDate=${endDateGC}"));
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		ReportDesign design = ReportManagerUtil.createExcelDesign("dd86d722-c9c9-4d23-beff-4e76ffbdc0b1", reportDefinition);
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
