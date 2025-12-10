package org.openmrs.module.mambaetl.reports.linelist;

import org.openmrs.module.mambaetl.datasetdefinition.linelist.EIDLineListDatasetDefinition;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.mambaetl.helpers.reportOptions.EIDAnalysisCategories;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.ReportRequest;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.reporting.report.manager.ReportManager;
import org.openmrs.module.reporting.report.manager.ReportManagerUtil;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class PMTCTEIDLineListReport implements ReportManager {
	
	@Override
	public String getUuid() {
		return "b2c3d4e5-f6g7-8901-2345-678901abcdef";
	}
	
	@Override
	public String getName() {
		return "LINELIST- PMTCT_EID";
	}
	
	@Override
	public String getDescription() {
		return "PMTCT_EID Line List";
	}
	
	@Override
    public List<Parameter> getParameters() {
		List<Parameter> parameters = new ArrayList<>(EthiOhriUtil.getDateRangeParameters(Boolean.TRUE));

		List<String> optionStrings = new ArrayList<>();
		for (EIDAnalysisCategories category : EIDAnalysisCategories.values()) {
			String value = category.getSqlValue();
			optionStrings.add(value);
		}
		String codedOptionsValue = String.join(",", optionStrings);
		Parameter reportType = new Parameter("reportType", "Report Type",
				String.class);
		reportType.addToWidgetConfiguration("codedOptions", codedOptionsValue);
		reportType.setDefaultValue(optionStrings.get(0));
		reportType.setRequired(true);

		parameters.add(reportType);
		return parameters;
    }
	
	@Override
	public ReportDefinition constructReportDefinition() {
		ReportDefinition reportDefinition = new ReportDefinition();
		reportDefinition.setUuid(getUuid());
		reportDefinition.setName(getName());
		reportDefinition.setDescription(getDescription());
		reportDefinition.setParameters(getParameters());
		
		EIDLineListDatasetDefinition eidLineListDatasetDefinition = new EIDLineListDatasetDefinition();
		eidLineListDatasetDefinition.addParameters(getParameters());
		reportDefinition.addDataSetDefinition("List of EID Clients", EthiOhriUtil.map(eidLineListDatasetDefinition,
		    "startDate=${startDateGC},endDate=${endDateGC},reportType=${reportType}"));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("g1h2i3j4-k5l6-9780-1234-567890abcdef", reportDefinition);
		design.setReportDefinition(reportDefinition);
		
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
