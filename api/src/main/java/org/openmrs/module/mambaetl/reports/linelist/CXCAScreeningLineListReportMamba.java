package org.openmrs.module.mambaetl.reports.linelist;

import org.openmrs.module.mambaetl.datasetdefinition.linelist.CXCAScreeningLineListDatasetDefinition;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
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
public class CXCAScreeningLineListReportMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "f719dae3-73b9-413c-b640-8341beb91812";
	}
	
	@Override
	public String getName() {
		return "MAMBA LINELIST- CX_CA_SCRN";
	}
	
	@Override
	public String getDescription() {
		return null;
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
		
		CXCAScreeningLineListDatasetDefinition cxcaScreeningLineListDatasetDefinition = new CXCAScreeningLineListDatasetDefinition();
		cxcaScreeningLineListDatasetDefinition.addParameters(getParameters());
		
		reportDefinition.addDataSetDefinition("List of Patients for Cervical Screening",
		    EthiOhriUtil.map(cxcaScreeningLineListDatasetDefinition, "startDate=${startDateGC},endDate=${endDateGC}"));
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("d42f1b4a-7373-4891-85e3-bddc5c90b8e4", reportDefinition);
		
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
