package org.openmrs.module.mambaetl.reports.linelist;

import org.openmrs.module.mambaetl.datasetdefinition.linelist.ProvidersViewLineListDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.linelist.TxCurrAnalysisLineListDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.mambaetl.helpers.reportOptions.TxCurrAnalysisCategories;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.ReportRequest;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.reporting.report.manager.ReportManager;
import org.openmrs.module.reporting.report.manager.ReportManagerUtil;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class ProvidersViewLineListReportMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "c0276e41-d668-41bc-a0e8-eeace2328376";
	}
	
	@Override
	public String getName() {
		return "MAMBA LINELIST- PROVERS VIEW";
	}
	
	@Override
	public String getDescription() {
		return null;
	}
	
	@Override
	public List<Parameter> getParameters() {
		
		// --- Configure this parameter to use your custom EthiopiaDateWidget ---
		// The exact key ("htmlWidget") depends on the rendering framework configuration,
		// but this is a common one for the HTML Widgets module.
		// The value is the fully qualified class name of your widget.
		
		Parameter startDate = new Parameter("startDate", "Start Date", Date.class);
		startDate.setRequired(true);
		Parameter startDateGC = new Parameter("startDateGC", " ", Date.class);
		startDateGC.setRequired(false);
		
		Parameter appointmentStartDate = new Parameter("appointmentStartDate", "Appointment Start Date", Date.class);
		appointmentStartDate.setRequired(true);
		Parameter appointmentStartDateGC = new Parameter("appointmentStartDateGC", " ", Date.class);
		appointmentStartDateGC.setRequired(false);
		
		Parameter endDate = new Parameter("endDate", "End Date", Date.class);
		endDate.setRequired(true);
		Parameter endDateGC = new Parameter("endDateGC", " ", Date.class);
		endDateGC.setRequired(false);
		//
		//		Parameter appointmentStartDate = new Parameter("appointmentStartDate", "Appointment Start Date", Date.class);
		//		appointmentStartDate.setRequired(true);
		//		Parameter appointmentStartDateGC = new Parameter("appointmentStartDateGC", " ", Date.class);
		//		appointmentStartDateGC.setRequired(false);
		//		Parameter appointmentEndDate = new Parameter("appointmentEndDate", "Appointment End Date", Date.class);
		//		appointmentEndDate.setRequired(true);
		//		Parameter appointmentEndDateGC = new Parameter("appointmentEndDateGC", " ", Date.class);
		//		appointmentEndDateGC.setRequired(false);
		
		//		Parameter clientType = new Parameter("clientType", "ART Client", String.class); // Parameter type is the ENUM class
		//		clientType.addToWidgetConfiguration("codedOptions", "all,curr"); // Use the generated string
		//		clientType.setRequired(true);
		//		clientType.setDefaultValue("all");
		//
		//		Parameter patientGUID = new Parameter("patientGUID", "Patient GUID", String.class);
		
		return Arrays.asList(startDate, startDateGC, appointmentStartDate, appointmentStartDateGC, endDate, endDateGC);
		
	}
	
	@Override
	public ReportDefinition constructReportDefinition() {
		ReportDefinition reportDefinition = new ReportDefinition();
		reportDefinition.setUuid(getUuid());
		reportDefinition.setName(getName());
		reportDefinition.setDescription(getDescription());
		
		reportDefinition.setParameters(getParameters());
		
		ProvidersViewLineListDataSetDefinitionMamba providersViewLineListDataSetDefinitionMamba = new ProvidersViewLineListDataSetDefinitionMamba();
		providersViewLineListDataSetDefinitionMamba.addParameters(getParameters());
		
		reportDefinition.addDataSetDefinition("List of Patients for Providers",
		    EthiOhriUtil.map(providersViewLineListDataSetDefinitionMamba, "appointmentStartDate=${appointmentStartDate}"));
		// ,endDate=${endDateGC},appointmentStartDate=${appointmentStartDateGC},appointmentEndDate=${appointmentEndDateGC},clientType=${clientType},patientGUID=${patientGUID}
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("b9139849-9cf8-4d73-8644-31c47c3d0fce", reportDefinition);
		
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
