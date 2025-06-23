package org.openmrs.module.mambaetl.reports.datim;

import org.openmrs.module.mambaetl.datasetdefinition.datim.HeaderDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tb_art.TBArtDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.mambaetl.helpers.reportOptions.TBArtAggregationTypes;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.ReportRequest;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.reporting.report.manager.ReportManager;
import org.openmrs.module.reporting.report.manager.ReportManagerUtil;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

@Component
public class TBARTDATIMReportsMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "53cd4c6c-5014-4222-ad7c-94da752fedc0";
	}
	
	@Override
	public String getName() {
		return "DATIM TREATMENT- TB_ART";
	}
	
	@Override
	public String getDescription() {
		return "TB ART DATIM mamba report";
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
		headerDefinition.setDescription("DSD: TB_ART");
		headerDefinition.setParameters(getParameters());
		reportDefinition.addDataSetDefinition("DSD: TB_ART", EthiOhriUtil.map(headerDefinition, "endDate=${endDateGC}"));
		
		TBArtDataSetDefinitionMamba tbArtTotalDataSetDefinitionMamba = new TBArtDataSetDefinitionMamba();
		tbArtTotalDataSetDefinitionMamba.addParameters(getParameters());
		tbArtTotalDataSetDefinitionMamba.setTbArtAggregationTypes(TBArtAggregationTypes.TOTAL);
		tbArtTotalDataSetDefinitionMamba
		        .setDescription("Number of TB cases with documented HIV-positive status who start or continue ART during the reporting period. Numerator will auto-calculate from Age/Sex/Result Disaggregates");
		reportDefinition
		        .addDataSetDefinition(
		            "Number of TB cases with documented HIV-positive status who start or continue ART during the reporting period. Numerator will auto-calculate from Age/Sex/Result Disaggregates",
		            EthiOhriUtil.map(tbArtTotalDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		TBArtDataSetDefinitionMamba tbArtDataSetDefinitionMamba = new TBArtDataSetDefinitionMamba();
		tbArtDataSetDefinitionMamba.addParameters(getParameters());
		tbArtDataSetDefinitionMamba.setTbArtAggregationTypes(TBArtAggregationTypes.ALREADY_ON_ART);
		tbArtDataSetDefinitionMamba.setDescription("Already on ART");
		reportDefinition.addDataSetDefinition("Already on ART",
		    EthiOhriUtil.map(tbArtDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		TBArtDataSetDefinitionMamba tbArtNewDataSetDefinitionMamba = new TBArtDataSetDefinitionMamba();
		tbArtNewDataSetDefinitionMamba.addParameters(getParameters());
		tbArtNewDataSetDefinitionMamba.setTbArtAggregationTypes(TBArtAggregationTypes.NEW_ON_ART);
		tbArtNewDataSetDefinitionMamba.setDescription("New on ART");
		reportDefinition.addDataSetDefinition("New on ART",
		    EthiOhriUtil.map(tbArtNewDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		ReportDesign design = ReportManagerUtil.createExcelDesign("9158bc48-e6ab-4228-aac5-ea5488b16031", reportDefinition);
		
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
