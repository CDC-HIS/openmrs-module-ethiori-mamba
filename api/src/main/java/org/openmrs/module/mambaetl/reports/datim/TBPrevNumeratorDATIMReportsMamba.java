package org.openmrs.module.mambaetl.reports.datim;

import org.openmrs.module.mambaetl.datasetdefinition.datim.HeaderDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tb_prev.TBPrevNumeratorDataSetDefinitionMamba;
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
public class TBPrevNumeratorDATIMReportsMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "a049528e-8a46-401f-8d19-ef0f3863db39";
	}
	
	@Override
	public String getName() {
		return "MAMBA DATIM PREVENTION- TB_PREV(DENOMINATOR)";
	}
	
	@Override
	public String getDescription() {
		return "TB_PREV(DENOMINATOR) DATIM mamba report";
	}
	
	@Override
	public List<Parameter> getParameters() {
		return EthiOhriUtil.getDateRangeParameters();
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
		
		TBPrevNumeratorDataSetDefinitionMamba tbPrevTotalNumeratorDataSetDefinitionMamba = new TBPrevNumeratorDataSetDefinitionMamba();
		tbPrevTotalNumeratorDataSetDefinitionMamba.addParameters(getParameters());
		tbPrevTotalNumeratorDataSetDefinitionMamba.setTBPrevAggregationTypes(TBPrevAggregationTypes.TOTAL);
		tbPrevTotalNumeratorDataSetDefinitionMamba
		        .setDescription("Among those who started a course of TPT in the previous reporting period, the number that completed a full course of therapy. (for continuous IPT programs, this includes the patients who have completed the first 6 months of isoniazid preventive therapy (IPT), or any other standard course of TPT such as 3 months of weekly isoniazid and rifapentine, or 3-HP)");
		reportDefinition
		        .addDataSetDefinition(
		            "Among those who started a course of TPT in the previous reporting period, the number that completed a full course of therapy. (for continuous IPT programs, this includes the patients who have completed the first 6 months of isoniazid preventive therapy (IPT), or any other standard course of TPT such as 3 months of weekly isoniazid and rifapentine, or 3-HP)",
		            EthiOhriUtil.map(tbPrevTotalNumeratorDataSetDefinitionMamba,
		                "startDate=${startDateGC},endDate=${endDateGC}"));
		
		TBPrevNumeratorDataSetDefinitionMamba tbPrevNumeratorDataSetDefinitionMamba = new TBPrevNumeratorDataSetDefinitionMamba();
		tbPrevNumeratorDataSetDefinitionMamba.addParameters(getParameters());
		tbPrevNumeratorDataSetDefinitionMamba.setTBPrevAggregationTypes(TBPrevAggregationTypes.PREV_ART);
		tbPrevNumeratorDataSetDefinitionMamba.setDescription("Disaggregated By ART Start by Age/Sex");
		reportDefinition.addDataSetDefinition("Disaggregated By ART Start by Age/Sex",
		    EthiOhriUtil.map(tbPrevNumeratorDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		ReportDesign design = ReportManagerUtil.createExcelDesign("1a5cfec5-7f87-4c75-ba7c-2d9c40804d3b", reportDefinition);
		
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
