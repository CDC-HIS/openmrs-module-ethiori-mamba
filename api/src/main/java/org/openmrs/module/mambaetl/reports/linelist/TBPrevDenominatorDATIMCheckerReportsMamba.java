package org.openmrs.module.mambaetl.reports.linelist;

import org.openmrs.module.mambaetl.datasetdefinition.datim.HeaderDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.linelist.TBPrevDenominatorCheckerDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.mambaetl.helpers.reportOptions.TBPrevAggregationTypes;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.ReportRequest;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.reporting.report.manager.ReportManager;
import org.openmrs.module.reporting.report.manager.ReportManagerUtil;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

//@Component
public class TBPrevDenominatorDATIMCheckerReportsMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "4fae8360-3aff-426a-9e3f-2fb44cf5e8a6";
	}
	
	@Override
	public String getName() {
		return "LINELIST- PREVENTION- TB_PREV(DENOMINATOR) Datim Checker";
	}
	
	@Override
	public String getDescription() {
		return "TB_PREV(DENOMINATOR) DATIM Checker mamba report";
	}
	
	@Override
	public List<Parameter> getParameters() {
		return EthiOhriUtil.getDateRangeParameters(Boolean.TRUE);
	}
	
	@Override
	public ReportDefinition constructReportDefinition() {
		ReportDefinition reportDefinition = new ReportDefinition();
		reportDefinition.setUuid(getUuid());
		reportDefinition.setName(getName());
		reportDefinition.setDescription(getDescription());
		reportDefinition.setParameters(getParameters());
		
		HeaderDataSetDefinitionMamba headerDefinition = new HeaderDataSetDefinitionMamba();
		headerDefinition.setDescription("DSD: TB_PREV(DENOMINATOR)");
		headerDefinition.setParameters(getParameters());
		reportDefinition.addDataSetDefinition("DSD: TB_PREV(DENOMINATOR)",
		    EthiOhriUtil.map(headerDefinition, "endDate=${endDateGC}"));
		
		TBPrevDenominatorCheckerDataSetDefinitionMamba tbPrevDenominatorCheckerDataSetDefinitionMamba = new TBPrevDenominatorCheckerDataSetDefinitionMamba();
		tbPrevDenominatorCheckerDataSetDefinitionMamba.addParameters(getParameters());
		tbPrevDenominatorCheckerDataSetDefinitionMamba.setTbPrevAggregationTypes(TBPrevAggregationTypes.DEBUG); // TBPrevAggregationTypes.PREV_ART is set to avoid NPE (actual logic in evaluator)
		tbPrevDenominatorCheckerDataSetDefinitionMamba
		        .setDescription("Number of ART patients who were initialized on any course of TPT during the previous reporting period");
		reportDefinition.addDataSetDefinition(
		    "Number of ART patients who were initialized on any course of TPT during the previous reporting period",
		    EthiOhriUtil
		            .map(tbPrevDenominatorCheckerDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		ReportDesign design = ReportManagerUtil.createExcelDesign("2cc4fb16-a00f-45ab-b1f6-f9a5bc6a9ff4", reportDefinition);
		design.setReportDefinition(reportDefinition);
		return Collections.singletonList(design);
	}
	
	@Override
	public List<ReportRequest> constructScheduledRequests(ReportDefinition reportDefinition) {
		return Collections.emptyList();
	}
	
	@Override
	public String getVersion() {
		return "1.0.0-SNAPSHOT";
	}
}
