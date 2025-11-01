package org.openmrs.module.mambaetl.reports.linelist;

import org.openmrs.module.mambaetl.datasetdefinition.linelist.ChronicCareEnrollmentDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.linelist.MissedAppointmentDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.mambaetl.helpers.EthiopianDate;
import org.openmrs.module.mambaetl.helpers.FollowUpConstant;
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
public class MissedAppointmentLineListReportMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "691327fb-9579-0202-920a-27ee765y43ef";
	}
	
	@Override
	public String getName() {
		return "LINELIST- Missed Appointment Tracing";
	}
	
	@Override
	public String getDescription() {
		return null;
	}
	
	@Override
	public List<Parameter> getParameters() {
		Date from = Date.from(ZonedDateTime.now().toInstant());
		EthiopianDate date = EthiOhriUtil.getEthiopiaDate(from);
		Parameter endDate = new Parameter("endDate", "End Date", Date.class);
		endDate.setRequired(true);
		endDate.setDefaultValue(date.getMonth() + "/" + date.getDay() + "/" + date.getYear());
		Parameter endDateGC = new Parameter("endDateGC", " ", Date.class);
		endDateGC.setRequired(false);
		endDateGC.setDefaultValue(from);
		return Arrays.asList(endDate, endDateGC);
	}
	
	@Override
	public ReportDefinition constructReportDefinition() {
		ReportDefinition reportDefinition = new ReportDefinition();
		reportDefinition.setUuid(getUuid());
		reportDefinition.setName(getName());
		reportDefinition.setDescription(getDescription());
		
		reportDefinition.setParameters(getParameters());
		
		MissedAppointmentDataSetDefinitionMamba dataSetDefinitionMamba = new MissedAppointmentDataSetDefinitionMamba();
		dataSetDefinitionMamba.addParameters(getParameters());
		
		reportDefinition.addDataSetDefinition("Missed Appointment line list",
		    map(dataSetDefinitionMamba, "endDate=${endDateGC}"));
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		ReportDesign design = ReportManagerUtil.createExcelDesign("a770d35a-mile-4910-a1dc-8149b7696817", reportDefinition);
		
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
