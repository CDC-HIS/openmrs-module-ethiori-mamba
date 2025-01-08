package org.openmrs.module.mambaetl.reports.linelist.migrated;

import java.sql.Date;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.openmrs.module.mambaetl.datasetdefinition.linelist.dataExtractionTool.CXCADataExtractionDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.ReportRequest;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.reporting.report.manager.ReportManager;
import org.openmrs.module.reporting.report.manager.ReportManagerUtil;
import org.springframework.stereotype.Component;

@Component
public class CxCaDataExtractionReport implements ReportManager {
	
	@Override
	public String getUuid() {
		return "8a0deea4-eb37-4f32-b058-8c24fb21621b";
	}
	
	@Override
	public String getName() {
		return "MAMBA LINELIST- CX_CA_Data_Extraction";
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
		
		CXCADataExtractionDataSetDefinitionMamba cxcaDataExtractionDatasetDefinition = new CXCADataExtractionDataSetDefinitionMamba();
		cxcaDataExtractionDatasetDefinition.addParameters(getParameters());
		
		reportDefinition.addDataSetDefinition("List of Patients for CXCA Data Extraction",
		    EthiOhriUtil.map(cxcaDataExtractionDatasetDefinition, "startDate=${startDateGC},endDate=${endDateGC}"));
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("73da2642-6075-49bb-9a67-8095e7a91ee7", reportDefinition);
		
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
