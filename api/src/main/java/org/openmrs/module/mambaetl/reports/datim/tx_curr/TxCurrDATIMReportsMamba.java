package org.openmrs.module.mambaetl.reports.datim.tx_curr;

import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_curr.TxCurrAgeSexDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_new.HeaderDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_new.KeyPopulationTypeDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_new.TxNewAgeSexCd4DataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.mambaetl.helpers.mapper.Cd4Status;
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
	} //4d7b385f-331f-400c-8592-f539f4565d9d
	
	@Override
	public String getName() {
		return "MAMBA DATIM- TX_CURR";
	}
	
	@Override
	public String getDescription() {
		return "TX curr DATIM mamba report ";
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
		headerDefinition.setDescription("DSD: TX_CURR");
		headerDefinition.setParameters(getParameters());
		reportDefinition.addDataSetDefinition("DSD: TX_CURR", EthiOhriUtil.map(headerDefinition, "endDate=${endDateGC}"));
		
		TxCurrAgeSexDataSetDefinitionMamba txCurrAgeSexDataSetDefinitionMamba = new TxCurrAgeSexDataSetDefinitionMamba();
		txCurrAgeSexDataSetDefinitionMamba.addParameters(getParameters());
		txCurrAgeSexDataSetDefinitionMamba.setDescription("Disaggregated by Age / Sex (Fine Disaggregate)");
		reportDefinition.addDataSetDefinition("Disaggregated by Age / Sex (Fine Disaggregate)",
		    EthiOhriUtil.map(txCurrAgeSexDataSetDefinitionMamba, "endDate=${endDateGC}"));

		KeyPopulationTypeDataSetDefinitionMamba keyPopulationTypeDataSetDefinitionMamba = new KeyPopulationTypeDataSetDefinitionMamba();
		keyPopulationTypeDataSetDefinitionMamba.addParameters(getParameters());
		reportDefinition.addDataSetDefinition("Disaggregated by key population type",
				EthiOhriUtil.map(keyPopulationTypeDataSetDefinitionMamba, "endDate=${endDateGC}"));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		ReportDesign design = ReportManagerUtil.createExcelDesign("dd86d722-c9c9-4d23-beff-4e76ffbdc0b1", reportDefinition);
		
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
