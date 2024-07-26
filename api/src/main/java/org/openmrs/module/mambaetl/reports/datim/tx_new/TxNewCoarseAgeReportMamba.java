package org.openmrs.module.mambaetl.reports.datim.tx_new;

import org.openmrs.module.mambaetl.datasetdefinition.datim.TxNewCoarseAgeDataSetDefinitionMamba;
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
public class TxNewCoarseAgeReportMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "5736fa07-e119-4ea2-b3c0-50fac959ba71";
	} //4d7b385f-331f-400c-8592-f539f4565d9d
	
	@Override
	public String getName() {
		return "DATIM- TX_NEW_COARSE_AGE";
	}
	
	@Override
	public String getDescription() {
		return "TX new Coarse Age DATIM mamba report ";
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
		
		TxNewCoarseAgeDataSetDefinitionMamba txNewCoarseAgeDataSetDefinitionMamba = new TxNewCoarseAgeDataSetDefinitionMamba();
		txNewCoarseAgeDataSetDefinitionMamba.addParameters(getParameters());
		reportDefinition.addDataSetDefinition("List of Patients With Coarse Age Group Aggregation",
		    EthiOhriUtil.map(txNewCoarseAgeDataSetDefinitionMamba));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("46ce413e-a331-4bef-a3c4-08571d393f6d", reportDefinition);
		
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
