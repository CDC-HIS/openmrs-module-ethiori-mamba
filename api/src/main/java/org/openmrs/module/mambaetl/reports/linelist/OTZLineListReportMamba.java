package org.openmrs.module.mambaetl.reports.linelist;

import org.openmrs.module.mambaetl.datasetdefinition.linelist.OTZLineListDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.linelist.TXNewLineListDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.ReportRequest;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.reporting.report.manager.ReportManager;
import org.openmrs.module.reporting.report.manager.ReportManagerUtil;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

@Component
public class OTZLineListReportMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "483e8f2f-9ecb-48a6-9df0-b4dabd00774d";
	} //4d7b385f-331f-400c-8592-f539f4565d9d
	
	@Override
	public String getName() {
		return "MAMBA LINELIST- OTZ";
	}
	
	@Override
	public String getDescription() {
		return "OTZ mamba implementation";
	}
	
	@Override
	public List<Parameter> getParameters() {
		return EthiOhriUtil.getDateRangeParameters(Boolean.FALSE);
		
	}
	
	@Override
	public ReportDefinition constructReportDefinition() {
		ReportDefinition reportDefinition = new ReportDefinition();
		reportDefinition.setUuid(getUuid());
		reportDefinition.setName(getName());
		reportDefinition.setDescription(getDescription());
		reportDefinition.setParameters(getParameters());
		
		OTZLineListDataSetDefinitionMamba otzLineListDataSetDefinitionMamba = new OTZLineListDataSetDefinitionMamba();
		otzLineListDataSetDefinitionMamba.addParameters(getParameters());
		reportDefinition.addDataSetDefinition("List of OTZ Clients",
		    EthiOhriUtil.map(otzLineListDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("60979d36-2fe1-45cd-a945-1e9c87f16ef6", reportDefinition);
		
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
