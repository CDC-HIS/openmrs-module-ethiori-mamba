package org.openmrs.module.mambaetl.reports.linelist.migrated;

import org.openmrs.module.mambaetl.datasetdefinition.migrated.CXCADatasetDefinition;
import org.openmrs.module.mambaetl.datasetdefinition.migrated.VLEligibilityDatasetDefinition;
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
public class VLEligibilityReport implements ReportManager {
	
	@Override
	public String getUuid() {
		return "fe00333e-bc89-4bfc-83e7-cce2847c864c";
	}
	
	@Override
	public String getName() {
		return "MAMBA LINELIST- VL_ELIGIBILITY";
	}
	
	@Override
	public String getDescription() {
		return null;
	}
	
	@Override
	public List<Parameter> getParameters() {
		Parameter startDate = new Parameter("startDate", "On Month", Date.class);
		startDate.setRequired(false);
		Parameter startDateGC = new Parameter("startDateGC", " ", Date.class);
		startDateGC.setRequired(false);
		Parameter endDate = new Parameter("endDate", "On Month", Date.class);
		endDate.setRequired(false);
		Parameter endDateGC = new Parameter("endDateGC", " ", Date.class);
		endDateGC.setRequired(false);
		return Arrays.asList(startDate, startDateGC, endDate, endDateGC);
		
	}
	
	@Override
	public ReportDefinition constructReportDefinition() {
		ReportDefinition reportDefinition = new ReportDefinition();
		reportDefinition.setUuid(getUuid());
		reportDefinition.setName(getName());
		reportDefinition.setDescription(getDescription());
		
		reportDefinition.setParameters(getParameters());
		
		VLEligibilityDatasetDefinition vlEligibilityDatasetDefinition = new VLEligibilityDatasetDefinition();
		vlEligibilityDatasetDefinition.addParameters(getParameters());
		
		reportDefinition.addDataSetDefinition("List of Patients eligible for VL",
		    EthiOhriUtil.map(vlEligibilityDatasetDefinition));
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("7c2822f5-997e-4557-a5ab-1f480097d66e", reportDefinition);
		
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
