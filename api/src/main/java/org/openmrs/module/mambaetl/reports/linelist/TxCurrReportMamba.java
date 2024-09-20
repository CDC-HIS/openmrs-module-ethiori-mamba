package org.openmrs.module.mambaetl.reports.linelist;

import org.openmrs.api.context.Context;
import org.openmrs.module.mambaetl.datasetdefinition.linelist.TxCurrDataSetDefinitionMamba;
import org.openmrs.module.reporting.evaluation.parameter.Mapped;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.evaluation.parameter.Parameterizable;
import org.openmrs.module.reporting.evaluation.parameter.ParameterizableUtil;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.ReportRequest;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.reporting.report.manager.ReportManager;
import org.openmrs.module.reporting.report.manager.ReportManagerUtil;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class TxCurrReportMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "86a28f40-7987-482b-bc49-2d32451d00d9";
	}
	
	@Override
	public String getName() {
		return "MAMBA LINELIST- TX_CURR";
	}
	
	@Override
	public String getDescription() {
		return null;
	}
	
	@Override
	public List<Parameter> getParameters() {
		
		Parameter endDate = new Parameter("endDate", "On Month", Date.class);
		endDate.setRequired(false);
		Parameter endDateGC = new Parameter("endDateGC", " ", Date.class);
		endDateGC.setRequired(false);
		return Arrays.asList(endDate, endDateGC);
		
	}
	
	@Override
	public ReportDefinition constructReportDefinition() {
		ReportDefinition reportDefinition = new ReportDefinition();
		reportDefinition.setUuid(getUuid());
		reportDefinition.setName(getName());
		reportDefinition.setDescription(getDescription());
		
		reportDefinition.setParameters(getParameters());
		
		TxCurrDataSetDefinitionMamba txCurrDataSetDefinition = new TxCurrDataSetDefinitionMamba();
		txCurrDataSetDefinition.addParameters(getParameters());
		
		reportDefinition.addDataSetDefinition("List of Patients Currently on ART",
		    map(txCurrDataSetDefinition, "endDate=${endDateGC}"));
		return reportDefinition;
	}
	
	public static <T extends Parameterizable> Mapped<T> map(T parameterizable, String mappings) {
		if (parameterizable == null) {
			throw new IllegalArgumentException("Parameterizable cannot be null");
		}
		if (mappings == null) {
			mappings = ""; // probably not necessary, just to be safe
		}
		return new Mapped<>(parameterizable, ParameterizableUtil.createParameterMappings(mappings));
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		ReportDesign design = ReportManagerUtil.createExcelDesign("a1ee4c33-f087-4af9-8efc-edf007e5c277", reportDefinition);
		
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
