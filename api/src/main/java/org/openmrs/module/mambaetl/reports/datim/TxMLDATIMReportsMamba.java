package org.openmrs.module.mambaetl.reports.datim;

import org.openmrs.module.mambaetl.datasetdefinition.datim.HeaderDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_ml.MLKeyPopulationDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_ml.TxMLDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.mambaetl.helpers.reportOptions.TxMLAggregationTypes;
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
public class TxMLDATIMReportsMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "d0465d88-3800-4477-82c8-97036269cb52";
	}
	
	@Override
	public String getName() {
		return "MAMBA DATIM TREATMENT- TX_ML";
	}
	
	@Override
	public String getDescription() {
		return "TX ML DATIM mamba report";
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
		headerDefinition.setDescription("DSD: TX_ML");
		headerDefinition.setParameters(getParameters());
		reportDefinition.addDataSetDefinition("DSD: TX_ML", EthiOhriUtil.map(headerDefinition, "endDate=${endDateGC}"));
		
		TxMLDataSetDefinitionMamba txMLTotalDataSetDefinitionMamba = new TxMLDataSetDefinitionMamba();
		txMLTotalDataSetDefinitionMamba.addParameters(getParameters());
		txMLTotalDataSetDefinitionMamba.setTxMLAggregationTypes(TxMLAggregationTypes.TOTAL);
		txMLTotalDataSetDefinitionMamba
		        .setDescription("Number of ART patients who were on ART at the beginning of the quarterly reporting period or initiated\n"
		                + "treatment during the reporting period and then had no clinical contact since their last expected contact. The\n"
		                + "numerator auto-calculates from the sum of Age/Sex Outcome");
		reportDefinition
		        .addDataSetDefinition(
		            "Number of ART patients who were on ART at the beginning of the quarterly reporting period or initiated\n"
		                    + "treatment during the reporting period and then had no clinical contact since their last expected contact. The\n"
		                    + "numerator auto-calculates from the sum of Age/Sex Outcome",
		            EthiOhriUtil.map(txMLTotalDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		HeaderDataSetDefinitionMamba secondHeader = new HeaderDataSetDefinitionMamba();
		secondHeader.setDescription("Disaggregated Outcome by Age/Sex");
		secondHeader.setParameters(getParameters());
		reportDefinition.addDataSetDefinition("Disaggregated Outcome by Age/Sex",
		    EthiOhriUtil.map(secondHeader, "endDate=${endDateGC}"));
		
		TxMLDataSetDefinitionMamba txMLDiedDataSetDefinitionMamba = new TxMLDataSetDefinitionMamba();
		txMLDiedDataSetDefinitionMamba.addParameters(getParameters());
		txMLDiedDataSetDefinitionMamba.setTxMLAggregationTypes(TxMLAggregationTypes.DIED);
		txMLDiedDataSetDefinitionMamba.setDescription("Died");
		reportDefinition.addDataSetDefinition("Died",
		    EthiOhriUtil.map(txMLDiedDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		TxMLDataSetDefinitionMamba txMLLessThan3MDataSetDefinitionMamba = new TxMLDataSetDefinitionMamba();
		txMLLessThan3MDataSetDefinitionMamba.addParameters(getParameters());
		txMLLessThan3MDataSetDefinitionMamba.setTxMLAggregationTypes(TxMLAggregationTypes.LESS_THAN_THREE_MONTHS);
		txMLLessThan3MDataSetDefinitionMamba
		        .setDescription("Interruption in Treatment After being on Treatment for < 3 months");
		reportDefinition.addDataSetDefinition("Interruption in Treatment After being on Treatment for < 3 months",
		    EthiOhriUtil.map(txMLLessThan3MDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		TxMLDataSetDefinitionMamba txML3TO5MDataSetDefinitionMamba = new TxMLDataSetDefinitionMamba();
		txML3TO5MDataSetDefinitionMamba.addParameters(getParameters());
		txML3TO5MDataSetDefinitionMamba.setTxMLAggregationTypes(TxMLAggregationTypes.THREE_TO_FIVE_MONTHS);
		txML3TO5MDataSetDefinitionMamba.setDescription("Interruption in Treatment After being on Treatment for 3-5 months");
		reportDefinition.addDataSetDefinition("Interruption in Treatment After being on Treatment for 3-5 months",
		    EthiOhriUtil.map(txML3TO5MDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		TxMLDataSetDefinitionMamba txMLM6MDataSetDefinitionMamba = new TxMLDataSetDefinitionMamba();
		txMLM6MDataSetDefinitionMamba.addParameters(getParameters());
		txMLM6MDataSetDefinitionMamba.setTxMLAggregationTypes(TxMLAggregationTypes.MORE_THAN_SIX_MONTHS);
		txMLM6MDataSetDefinitionMamba.setDescription("Interruption in Treatment After being on Treatment for 6+ months");
		reportDefinition.addDataSetDefinition("Interruption in Treatment After being on Treatment for 6+ months",
		    EthiOhriUtil.map(txMLM6MDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		TxMLDataSetDefinitionMamba txMLTODataSetDefinitionMamba = new TxMLDataSetDefinitionMamba();
		txMLTODataSetDefinitionMamba.addParameters(getParameters());
		txMLTODataSetDefinitionMamba.setTxMLAggregationTypes(TxMLAggregationTypes.TRANSFERRED_OUT);
		txMLTODataSetDefinitionMamba.setDescription("Transferred Out");
		reportDefinition.addDataSetDefinition("Transferred Out",
		    EthiOhriUtil.map(txMLTODataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		TxMLDataSetDefinitionMamba txMLRefusedDataSetDefinitionMamba = new TxMLDataSetDefinitionMamba();
		txMLRefusedDataSetDefinitionMamba.addParameters(getParameters());
		txMLRefusedDataSetDefinitionMamba.setTxMLAggregationTypes(TxMLAggregationTypes.REFUSED);
		txMLRefusedDataSetDefinitionMamba.setDescription("Refused (Stopped) Treatment");
		reportDefinition.addDataSetDefinition("Refused (Stopped) Treatment",
		    EthiOhriUtil.map(txMLRefusedDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
//		MLKeyPopulationDataSetDefinitionMamba mlKeyPopulationDataSetDefinitionMamba = new MLKeyPopulationDataSetDefinitionMamba();
//		mlKeyPopulationDataSetDefinitionMamba.addParameters(getParameters());
//		mlKeyPopulationDataSetDefinitionMamba.setDescription("Disaggregated by Status/Key Population Type:");
//		reportDefinition.addDataSetDefinition("Disaggregated by Status/Key Population Type:",
//		    EthiOhriUtil.map(mlKeyPopulationDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("fee9078b-63e7-424d-86a7-5647fab5fb34", reportDefinition);
		
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
