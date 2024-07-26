package org.openmrs.module.mambaetl.reports.datim.tx_new;

import org.openmrs.module.mambaetl.datasetdefinition.datim.TxNewCD4DataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.linelist.TXNewDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
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
public class TxNewCD4ReportMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "872b7ebd-ddc7-41dc-b9be-f3d44ec4ec74";
	} //4d7b385f-331f-400c-8592-f539f4565d9d
	
	@Override
	public String getName() {
		return "DATIM- TX_NEW_CD4";
	}
	
	@Override
	public String getDescription() {
		return "TX new CD4 DATIM mamba report ";
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
		
		TxNewCD4DataSetDefinitionMamba txNewCD4DataSetDefinitionMamba = new TxNewCD4DataSetDefinitionMamba();
		txNewCD4DataSetDefinitionMamba.addParameters(getParameters());
		reportDefinition.addDataSetDefinition("List of Patients With CD4 Count Aggregation",
		    EthiOhriUtil.map(txNewCD4DataSetDefinitionMamba));
		
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
