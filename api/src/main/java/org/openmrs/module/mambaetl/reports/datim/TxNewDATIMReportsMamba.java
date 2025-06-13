package org.openmrs.module.mambaetl.reports.datim;

import org.openmrs.module.mambaetl.datasetdefinition.datim.HeaderDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.KeyPopulationDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_new.TxNewAgeSexCd4DataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.mambaetl.helpers.reportOptions.TxNewAggregationTypes;
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
public class TxNewDATIMReportsMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "872b7ebd-ddc7-41dc-b9be-f3d44ec4ec74";
	} //4d7b385f-331f-400c-8592-f539f4565d9d
	
	@Override
	public String getName() {
		return "MAMBA DATIM TREATMENT- TX_NEW";
	}
	
	@Override
	public String getDescription() {
		return "TX new DATIM mamba report ";
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
		headerDefinition.setDescription("DSD: TX_NEW");
		headerDefinition.setParameters(getParameters());
		reportDefinition.addDataSetDefinition("DSD: TX_NEW", EthiOhriUtil.map(headerDefinition, "endDate=${endDateGC}"));
		
		TxNewAgeSexCd4DataSetDefinitionMamba numeratorDataSetDefinition = new TxNewAgeSexCd4DataSetDefinitionMamba();
		numeratorDataSetDefinition.addParameters(getParameters());
		numeratorDataSetDefinition.setTxNewAggregationType(TxNewAggregationTypes.NUMERATOR);
		numeratorDataSetDefinition
		        .setDescription("Auto-Calculate Number of adults and children newly enrolled on antiretroviral therapy (ART). Numerator will auto-calculate from Age/Sex Disaggregates.");
		reportDefinition
		        .addDataSetDefinition(
		            "Auto-Calculate Number of adults and children newly enrolled on antiretroviral therapy (ART). Numerator will auto-calculate from Age/Sex Disaggregates.",
		            EthiOhriUtil.map(numeratorDataSetDefinition, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		HeaderDataSetDefinitionMamba headerDefinition2 = new HeaderDataSetDefinitionMamba();
		headerDefinition2.setDescription("Required Disaggregated by Age/Sex And CD4");
		headerDefinition2.setParameters(getParameters());
		reportDefinition.addDataSetDefinition("Required Disaggregated by Age/Sex And CD4",
		    EthiOhriUtil.map(headerDefinition2, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		TxNewAgeSexCd4DataSetDefinitionMamba lessThan200CD4DataSetDefinitionMamba = new TxNewAgeSexCd4DataSetDefinitionMamba();
		lessThan200CD4DataSetDefinitionMamba.addParameters(getParameters());
		lessThan200CD4DataSetDefinitionMamba.setTxNewAggregationType(TxNewAggregationTypes.LOW);
		lessThan200CD4DataSetDefinitionMamba.setDescription("<200 CD4");
		reportDefinition.addDataSetDefinition("<200 CD4",
		    EthiOhriUtil.map(lessThan200CD4DataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		TxNewAgeSexCd4DataSetDefinitionMamba greaterThan200CD4DataSetDefinitionMamba = new TxNewAgeSexCd4DataSetDefinitionMamba();
		greaterThan200CD4DataSetDefinitionMamba.addParameters(getParameters());
		greaterThan200CD4DataSetDefinitionMamba.setTxNewAggregationType(TxNewAggregationTypes.HIGH);
		greaterThan200CD4DataSetDefinitionMamba.setDescription(">= 200 CD4");
		reportDefinition.addDataSetDefinition(">= 200 CD4",
		    EthiOhriUtil.map(greaterThan200CD4DataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		TxNewAgeSexCd4DataSetDefinitionMamba unknownCD4DataSetDefinitionMamba = new TxNewAgeSexCd4DataSetDefinitionMamba();
		unknownCD4DataSetDefinitionMamba.addParameters(getParameters());
		unknownCD4DataSetDefinitionMamba.setTxNewAggregationType(TxNewAggregationTypes.UNKNOWN);
		reportDefinition.addDataSetDefinition("Unknown CD4",
		    EthiOhriUtil.map(unknownCD4DataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		TxNewAgeSexCd4DataSetDefinitionMamba breastFeedingStatusDataSetDefinitionMamba = new TxNewAgeSexCd4DataSetDefinitionMamba();
		breastFeedingStatusDataSetDefinitionMamba.addParameters(getParameters());
		breastFeedingStatusDataSetDefinitionMamba.setTxNewAggregationType(TxNewAggregationTypes.BREAST_FEEDING);
		reportDefinition.addDataSetDefinition("Disaggregated by Breastfeeding Status at ART Initiation",
		    EthiOhriUtil.map(breastFeedingStatusDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		KeyPopulationDataSetDefinitionMamba keyPopulationDataSetDefinitionMamba = new KeyPopulationDataSetDefinitionMamba();
		keyPopulationDataSetDefinitionMamba.addParameters(getParameters());
		reportDefinition.addDataSetDefinition("Disaggregated by key population type",
		    EthiOhriUtil.map(keyPopulationDataSetDefinitionMamba, "endDate=${endDateGC}"));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("6d4cc920-e087-4b1c-9af2-287c80c0e0f8", reportDefinition);
		
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
