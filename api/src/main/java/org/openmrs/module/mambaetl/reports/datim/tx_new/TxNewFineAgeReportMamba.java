package org.openmrs.module.mambaetl.reports.datim.tx_new;

import org.openmrs.module.mambaetl.datasetdefinition.datim.TxNewFineAgeDataSetDefinitionMamba;
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
public class TxNewFineAgeReportMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "dc8b9499-ea4d-4dbc-9221-a0ac07d2408c";
	} //4d7b385f-331f-400c-8592-f539f4565d9d
	
	@Override
	public String getName() {
		return "DATIM- TX_NEW_FINE_AGE";
	}
	
	@Override
	public String getDescription() {
		return "TX new Fine Age DATIM mamba report ";
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
		
		TxNewFineAgeDataSetDefinitionMamba txNewFineAgeDataSetDefinitionMamba = new TxNewFineAgeDataSetDefinitionMamba();
		txNewFineAgeDataSetDefinitionMamba.addParameters(getParameters());
		reportDefinition.addDataSetDefinition("List of Patients With Fine Age Group Aggregation",
		    EthiOhriUtil.map(txNewFineAgeDataSetDefinitionMamba));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("d1d5bc28-32fc-494e-8d4f-6d3c73f28182", reportDefinition);
		
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
