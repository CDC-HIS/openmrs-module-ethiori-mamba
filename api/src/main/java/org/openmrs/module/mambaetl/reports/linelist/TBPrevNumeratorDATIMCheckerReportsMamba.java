package org.openmrs.module.mambaetl.reports.linelist;

import org.openmrs.module.mambaetl.datasetdefinition.datim.HeaderDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.linelist.TBPrevNumeratorCheckerDataSetDefinitionMamba;
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

@Component
public class TBPrevNumeratorDATIMCheckerReportsMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "e68a7c75-3402-43eb-912a-8cd1bc8bcd5f";
	}
	
	@Override
	public String getName() {
		return "LINELIST- TB_PREV(NUMERATOR) Datim Checker";
	}
	
	@Override
	public String getDescription() {
		return "TB_PREV(NUMERATOR) DATIM Checker mamba report";
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
		headerDefinition.setDescription("DSD: TB_PREV(NUMERATOR)");
		headerDefinition.setParameters(getParameters());
		reportDefinition.addDataSetDefinition("DSD: TB_PREV(NUMERATOR)",
		    EthiOhriUtil.map(headerDefinition, "endDate=${endDateGC}"));
		
		TBPrevNumeratorCheckerDataSetDefinitionMamba tbPrevNumeratorCheckerDataSetDefinitionMamba = new TBPrevNumeratorCheckerDataSetDefinitionMamba();
		tbPrevNumeratorCheckerDataSetDefinitionMamba.addParameters(getParameters());
		tbPrevNumeratorCheckerDataSetDefinitionMamba.setTbPrevAggregationTypes(TBPrevAggregationTypes.DEBUG); // TBPrevAggregationTypes.PREV_ART is set to avoid NPE (actual logic in evaluator)
		tbPrevNumeratorCheckerDataSetDefinitionMamba
		        .setDescription("Among those who started a course of TPT in the previous reporting period, the number that completed a full course of therapy. (for continuous IPT programs, this includes the patients who have completed the first 6 months of isoniazid preventive therapy (IPT), or any other standard course of TPT such as 3 months of weekly isoniazid and rifapentine, or 3-HP)");
		reportDefinition
		        .addDataSetDefinition(
		            "Among those who started a course of TPT in the previous reporting period, the number that completed a full course of therapy. (for continuous IPT programs, this includes the patients who have completed the first 6 months of isoniazid preventive therapy (IPT), or any other standard course of TPT such as 3 months of weekly isoniazid and rifapentine, or 3-HP)",
		            EthiOhriUtil.map(tbPrevNumeratorCheckerDataSetDefinitionMamba,
		                "startDate=${startDateGC},endDate=${endDateGC}"));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		ReportDesign design = ReportManagerUtil.createExcelDesign("7fbe039f-05f7-42ef-94bb-a08f50c2a435", reportDefinition);
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
