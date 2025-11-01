package org.openmrs.module.mambaetl.reports.linelist;

import org.openmrs.module.mambaetl.datasetdefinition.linelist.PEPLineListDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.linelist.TiToLineListDataSetDefinitionMamba;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.ReportRequest;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.reporting.report.manager.ReportManager;
import org.openmrs.module.reporting.report.manager.ReportManagerUtil;
import org.springframework.stereotype.Component;

import java.util.*;

import static org.openmrs.module.mambaetl.helpers.EthiOhriUtil.map;

@Component
public class TITOLineListReportMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "87654321-1111-7623-920a-27ee7655ebef";
	}
	
	@Override
	public String getName() {
		return "LINELIST- Transferred In/Out";
	}
	
	@Override
	public String getDescription() {
		return null;
	}
	
	@Override
	public List<Parameter> getParameters() {
		Parameter status = new Parameter("status", "Transfer status", String.class);
		status.addToWidgetConfiguration("codedOptions", "TI" + "," + "TO");
		status.setRequired(true);
		
		Parameter startDate = new Parameter("startDate", "Start Date", Date.class);
		startDate.setRequired(false);
		Parameter startDateGC = new Parameter("startDateGC", " ", Date.class);
		startDateGC.setRequired(false);
		Parameter endDate = new Parameter("endDate", "End Date", Date.class);
		endDate.setRequired(false);
		Parameter endDateGC = new Parameter("endDateGC", " ", Date.class);
		endDateGC.setRequired(false);
		
		return Arrays.asList(startDate, startDateGC, endDate, endDateGC, status);
		
	}
	
	@Override
	public ReportDefinition constructReportDefinition() {
		ReportDefinition reportDefinition = new ReportDefinition();
		reportDefinition.setUuid(getUuid());
		reportDefinition.setName(getName());
		reportDefinition.setDescription(getDescription());
		
		reportDefinition.setParameters(getParameters());
		
		TiToLineListDataSetDefinitionMamba dataSetDefinitionMamba = new TiToLineListDataSetDefinitionMamba();
		dataSetDefinitionMamba.addParameters(getParameters());
		
		reportDefinition.addDataSetDefinition("Transferred In/Out",
		    map(dataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC},status=${status}"));
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		ReportDesign design = ReportManagerUtil
		        .createExcelDesign("89456321-1100-000222-a1dc-8149b7696817", reportDefinition);
		
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
