package org.openmrs.module.mambaetl.reports.datim;

import org.openmrs.module.mambaetl.datasetdefinition.datim.HeaderDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tb_prev.TBPrevDenominatorDataSetDefinitionMamba;
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
public class TBPrevDenominatorDATIMReportsMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "ea74201e-df82-4ffa-bb7c-f73e739c0694";
	}
	
	@Override
	public String getName() {
		return "DATIM PREVENTION- TB_PREV(DENOMINATOR)";
	}
	
	@Override
	public String getDescription() {
		return "TB_PREV(DENOMINATOR) DATIM mamba report";
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
		
		TBPrevDenominatorDataSetDefinitionMamba tbPrevTotalDenominatorDataSetDefinitionMamba = new TBPrevDenominatorDataSetDefinitionMamba();
		tbPrevTotalDenominatorDataSetDefinitionMamba.addParameters(getParameters());
		tbPrevTotalDenominatorDataSetDefinitionMamba.setTbPrevAggregationTypes(TBPrevAggregationTypes.TOTAL);
		tbPrevTotalDenominatorDataSetDefinitionMamba
		        .setDescription("Number of ART patients who were initialized on any course of TPT during the previous reporting period");
		reportDefinition.addDataSetDefinition(
		    "Number of ART patients who were initialized on any course of TPT during the previous reporting period",
		    EthiOhriUtil.map(tbPrevTotalDenominatorDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		TBPrevDenominatorDataSetDefinitionMamba tbPrevDenominatorDataSetDefinitionMamba = new TBPrevDenominatorDataSetDefinitionMamba();
		tbPrevDenominatorDataSetDefinitionMamba.addParameters(getParameters());
		tbPrevDenominatorDataSetDefinitionMamba.setTbPrevAggregationTypes(TBPrevAggregationTypes.PREV_ART);
		tbPrevDenominatorDataSetDefinitionMamba.setDescription("Disaggregated By ART Start by Age/Sex");
		reportDefinition.addDataSetDefinition("Disaggregated By ART Start by Age/Sex",
		    EthiOhriUtil.map(tbPrevDenominatorDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		ReportDesign design = ReportManagerUtil.createExcelDesign("73eaa74d-ac8c-4a93-bd18-802a2d1a7def", reportDefinition);
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
