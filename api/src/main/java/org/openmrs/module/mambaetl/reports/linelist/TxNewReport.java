package org.openmrs.module.mambaetl.reports.linelist;

import org.jetbrains.annotations.NotNull;
import org.openmrs.api.context.Context;
import org.openmrs.module.mambaetl.datasetdefinition.linelist.HTSNewDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.ReportRequest;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.reporting.report.manager.ReportManager;
import org.openmrs.module.reporting.report.manager.ReportManagerUtil;
import org.springframework.stereotype.Component;

import java.sql.Date;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Component
public class TxNewReport implements ReportManager {
	
	@Override
	public String getUuid() {
		return "4d7b385f-331f-400c-8592-f539f45657uy";
	} //4d7b385f-331f-400c-8592-f539f4565d9d
	
	@Override
	public String getName() {
		return "LINELIST - TX_NEW_MAMBA_MAIN";
	}
	
	@Override
	public String getDescription() {
		return "TX new mamba implementation";
	}
	
	@Override
	public List<Parameter> getParameters() {
		System.out.println("getting parameters");
		return EthiOhriUtil.getDateRangeParameters();
		
	}
	
	@Override
	public ReportDefinition constructReportDefinition() {
		ReportDefinition reportDefinition = new ReportDefinition();
		reportDefinition.setUuid(getUuid());
		reportDefinition.setName(getName());
		reportDefinition.setDescription(getDescription());
		reportDefinition.setParameters(getParameters());
		
		HTSNewDataSetDefinitionMamba htsNewDataSetDefinition = new HTSNewDataSetDefinitionMamba();
		htsNewDataSetDefinition.addParameters(getParameters());
		reportDefinition.addDataSetDefinition("List of Patients Newly Started ART",
		    EthiOhriUtil.map(htsNewDataSetDefinition));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("a4ae3c0a-bad8-4efe-8b8d-c2762c13f013", reportDefinition);
		
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
	
	//	@NotNull
	//	private List<Parameter> getDateRangeParameters() {
	//		Parameter startDate = new Parameter("startDate", "Start Date", Date.class);
	//		startDate.setRequired(true);
	//		Parameter startDateGC = new Parameter("startDateGC", " ", Date.class);
	//		startDateGC.setRequired(false);
	//		Parameter endDate = new Parameter("endDate", "End Date", Date.class);
	//		endDate.setRequired(true);
	//		Parameter endDateGC = new Parameter("endDateGC", " ", Date.class);
	//		endDateGC.setRequired(false);
	//		return Arrays.asList(startDate, startDateGC, endDate, endDateGC);
	//	}
}
