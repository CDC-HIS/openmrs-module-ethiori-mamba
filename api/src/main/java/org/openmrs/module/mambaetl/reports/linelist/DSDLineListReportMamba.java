package org.openmrs.module.mambaetl.reports.linelist;

import org.openmrs.module.mambaetl.datasetdefinition.linelist.DSDDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.linelist.MissedAppointmentDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.mambaetl.helpers.EthiopianDate;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.ReportRequest;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.reporting.report.manager.ReportManager;
import org.openmrs.module.reporting.report.manager.ReportManagerUtil;
import org.springframework.stereotype.Component;

import java.time.ZonedDateTime;
import java.util.*;

import static org.openmrs.module.mambaetl.helpers.EthiOhriUtil.map;

@Component
public class DSDLineListReportMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "691327fb-9579-0202-0202-27ee765y43ef";
	}
	
	@Override
	public String getName() {
		return "LINELIST- DSD-Mamba";
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
		return Arrays.asList(startDate, startDateGC, endDate, endDateGC);
	}
	
	@Override
	public ReportDefinition constructReportDefinition() {
		ReportDefinition reportDefinition = new ReportDefinition();
		reportDefinition.setUuid(getUuid());
		reportDefinition.setName(getName());
		reportDefinition.setDescription(getDescription());
		
		reportDefinition.setParameters(getParameters());
		
		DSDDataSetDefinitionMamba dataSetDefinitionMamba = new DSDDataSetDefinitionMamba();
		dataSetDefinitionMamba.addParameters(getParameters());
		
		reportDefinition.addDataSetDefinition("DSD mamba line list",
		    map(dataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		ReportDesign design = ReportManagerUtil.createExcelDesign("a770d35a-0202-4910-a1dc-8149b7696817", reportDefinition);
		
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
