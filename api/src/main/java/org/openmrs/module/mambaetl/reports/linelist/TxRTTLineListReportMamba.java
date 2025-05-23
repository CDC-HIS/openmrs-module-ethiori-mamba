package org.openmrs.module.mambaetl.reports.linelist;

import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_rtt.TxRTTDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.linelist.TXNewLineListDataSetDefinitionMamba;
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
public class TxRTTLineListReportMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "5a70cb95-d971-4201-a476-6e4b5790537c";
	} //4d7b385f-331f-400c-8592-f539f4565d9d
	
	@Override
	public String getName() {
		return "MAMBA LINELIST- TX_RTT";
	}
	
	@Override
	public String getDescription() {
		return "TX RTT";
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
		
		TxRTTDataSetDefinitionMamba txRTTDataSetDefinitionMamba = new TxRTTDataSetDefinitionMamba();
		txRTTDataSetDefinitionMamba.addParameters(getParameters());
		reportDefinition.addDataSetDefinition("List of RTT Patients",
		    EthiOhriUtil.map(txRTTDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("1343d7c5-7302-4568-b68f-8732e6ed645d", reportDefinition);
		
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
