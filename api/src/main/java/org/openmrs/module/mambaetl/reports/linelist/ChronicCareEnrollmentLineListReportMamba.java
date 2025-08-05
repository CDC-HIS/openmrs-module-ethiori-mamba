package org.openmrs.module.mambaetl.reports.linelist;

import org.openmrs.module.mambaetl.datasetdefinition.linelist.ChronicCareEnrollmentDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.FollowUpConstant;
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
public class ChronicCareEnrollmentLineListReportMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "691327fb-9579-4c18-920a-27ee765y43ef";
	}
	
	@Override
	public String getName() {
		return "LINELIST- Chronic-Mamba";
	}
	
	@Override
	public String getDescription() {
		return null;
	}
	
	@Override
	public List<Parameter> getParameters() {
		Parameter startDate = new Parameter("startDate", "Start Date", Date.class);
		startDate.setRequired(false);
		Parameter startDateGC = new Parameter("startDateGC", " ", Date.class);
		startDateGC.setRequired(false);
		Parameter endDate = new Parameter("endDate", "End Date", Date.class);
		endDate.setRequired(false);
		Parameter endDateGC = new Parameter("endDateGC", " ", Date.class);
		endDateGC.setRequired(false);
		
		Parameter followUpStatus = new Parameter("followupStatus", "Followup Status", String.class);
		followUpStatus.addToWidgetConfiguration("codedOptions", FollowUpConstant.getAllListOfStatus());
		followUpStatus.setRequired(false);
		
		return Arrays.asList(startDate, startDateGC, endDate, endDateGC, followUpStatus);
	}
	
	@Override
	public ReportDefinition constructReportDefinition() {
		ReportDefinition reportDefinition = new ReportDefinition();
		reportDefinition.setUuid(getUuid());
		reportDefinition.setName(getName());
		reportDefinition.setDescription(getDescription());
		
		reportDefinition.setParameters(getParameters());
		
		ChronicCareEnrollmentDataSetDefinitionMamba dataSetDefinitionMamba = new ChronicCareEnrollmentDataSetDefinitionMamba();
		dataSetDefinitionMamba.addParameters(getParameters());
		
		reportDefinition.addDataSetDefinition("Chronic Care mamba line list",
		    map(dataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC},followupStatus=${followupStatus}"));
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		ReportDesign design = ReportManagerUtil.createExcelDesign("a770d35a-kilo-4910-a1dc-8149b7696817", reportDefinition);
		
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
