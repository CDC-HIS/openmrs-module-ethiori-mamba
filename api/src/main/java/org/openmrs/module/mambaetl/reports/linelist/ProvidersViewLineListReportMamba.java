package org.openmrs.module.mambaetl.reports.linelist;

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
		Parameter startDate = new Parameter("startDate", "Start Date", Date.class);
		startDate.setRequired(true);
		Parameter startDateGC = new Parameter("startDateGC", " ", Date.class);
		startDateGC.setRequired(false);
		Parameter endDate = new Parameter("endDate", "End Date", Date.class);
		endDate.setRequired(true);
		Parameter endDateGC = new Parameter("endDateGC", " ", Date.class);
		endDateGC.setRequired(false);
		List<String> optionStrings = new ArrayList<>();
		for (TxCurrAnalysisCategories category : TxCurrAnalysisCategories.values()) {
			String value = category.getSqlValue(); // Or category.name() if preferred as backend value
			optionStrings.add(value);
		}

		String codedOptionsValue = String.join(",", optionStrings);

		Parameter txCurrAnalysisCategories = new Parameter("txCurrAnalysisCategories", "Analysis Group",
				TxCurrAnalysisCategories.class); // Parameter type is the ENUM class
		txCurrAnalysisCategories.addToWidgetConfiguration("codedOptions", codedOptionsValue); // Use the generated string
		txCurrAnalysisCategories.setRequired(true);
		return Arrays.asList(startDate, startDateGC, endDate, endDateGC, txCurrAnalysisCategories);
		
	}
	
	@Override
	public ReportDefinition constructReportDefinition() {
		ReportDefinition reportDefinition = new ReportDefinition();
		reportDefinition.setUuid(getUuid());
		reportDefinition.setName(getName());
		reportDefinition.setDescription(getDescription());
		
		reportDefinition.setParameters(getParameters());
		
		TxCurrAnalysisLineListDataSetDefinitionMamba txCurrAnalysisDataSetDefinition = new TxCurrAnalysisLineListDataSetDefinitionMamba();
		txCurrAnalysisDataSetDefinition.addParameters(getParameters());
		
		reportDefinition.addDataSetDefinition("List of Patients for Providers", EthiOhriUtil.map(
		    txCurrAnalysisDataSetDefinition,
		    "startDate=${startDateGC},endDate=${endDateGC},txCurrAnalysisCategories=${txCurrAnalysisCategories}"));
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
