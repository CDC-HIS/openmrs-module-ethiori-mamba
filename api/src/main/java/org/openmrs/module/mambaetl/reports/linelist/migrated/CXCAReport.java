package org.openmrs.module.mambaetl.reports.linelist.migrated;

import org.openmrs.module.mambaetl.datasetdefinition.migrated.CXCADatasetDefinition;
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
public class CXCAReport implements ReportManager {
	
	@Override
	public String getUuid() {
		return "2c3015a0-7f61-4b48-a370-2846b5dfdc6f";
	}
	
	@Override
	public String getName() {
		return "MAMBA LINELIST- CX_CA";
	}
	
	@Override
	public String getDescription() {
		return null;
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
		
		CXCADatasetDefinition cxcaDatasetDefinition = new CXCADatasetDefinition();
		cxcaDatasetDefinition.addParameters(getParameters());
		
		reportDefinition.addDataSetDefinition("List of Patients for CXCA",
		    EthiOhriUtil.map(cxcaDatasetDefinition, "startDate=${startDateGC},endDate=${endDateGC}"));
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("4d0ad55c-e116-4c5b-811b-991d59319a53", reportDefinition);
		
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
