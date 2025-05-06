package org.openmrs.module.mambaetl.reports.datim;

import org.openmrs.module.mambaetl.datasetdefinition.datim.HeaderDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.cxca.CXCASCRNDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.cxca.CXCATXDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.mambaetl.helpers.reportOptions.CXCATXAggregationTypes;
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
public class CXCASCRNDATIMReportsMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "be8377b4-cad5-4aea-91c8-8ee72f90cb7b";
	} //4d7b385f-331f-400c-8592-f539f4565d9d
	
	@Override
	public String getName() {
		return "MAMBA-DATIM-TESTING- CXCA_SCRN";
	}
	
	@Override
	public String getDescription() {
		return "CXCA_SCRN DATIM mamba report ";
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
		headerDefinition.setDescription("DSD: CXCA_SCRN");
		headerDefinition.setParameters(getParameters());
		reportDefinition.addDataSetDefinition("DSD: CXCA_SCRN", EthiOhriUtil.map(headerDefinition, "endDate=${endDateGC}"));
		
		CXCASCRNDataSetDefinitionMamba cxcaScrnFirstTimeDataSetDefinitionMamba = new CXCASCRNDataSetDefinitionMamba();
		cxcaScrnFirstTimeDataSetDefinitionMamba.addParameters(getParameters());
		cxcaScrnFirstTimeDataSetDefinitionMamba.setCxcatxAggregationTypes(CXCATXAggregationTypes.FIRST_TIME_SCREENING);
		cxcaScrnFirstTimeDataSetDefinitionMamba.setDescription("First time screened for cervical cancer");
		reportDefinition.addDataSetDefinition("First time screened for cervical cancer",
		    EthiOhriUtil.map(cxcaScrnFirstTimeDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		CXCASCRNDataSetDefinitionMamba cxcaScrnRescreenedDataSetDefinitionMamba = new CXCASCRNDataSetDefinitionMamba();
		cxcaScrnRescreenedDataSetDefinitionMamba.addParameters(getParameters());
		cxcaScrnRescreenedDataSetDefinitionMamba.setCxcatxAggregationTypes(CXCATXAggregationTypes.RE_SCREENING);
		cxcaScrnRescreenedDataSetDefinitionMamba.setDescription("Rescreened after previous negative or suspected cancer");
		reportDefinition.addDataSetDefinition("Rescreened after previous negative or suspected cancer",
		    EthiOhriUtil.map(cxcaScrnRescreenedDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		CXCASCRNDataSetDefinitionMamba cxcaScrnPostTreatmentDataSetDefinitionMamba = new CXCASCRNDataSetDefinitionMamba();
		cxcaScrnPostTreatmentDataSetDefinitionMamba.addParameters(getParameters());
		cxcaScrnPostTreatmentDataSetDefinitionMamba.setCxcatxAggregationTypes(CXCATXAggregationTypes.POST_TREATMENT);
		cxcaScrnPostTreatmentDataSetDefinitionMamba.setDescription("Post-treatment follow-up");
		reportDefinition.addDataSetDefinition("Post-treatment follow-up",
		    EthiOhriUtil.map(cxcaScrnPostTreatmentDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("e4a79da6-6950-421a-bab3-20a547c06295", reportDefinition);
		
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
