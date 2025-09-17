package org.openmrs.module.mambaetl.reports.datim;

import org.openmrs.module.mambaetl.datasetdefinition.datim.HeaderDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.cxca.CXCATXDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.mambaetl.helpers.reportOptions.CXCATXAggregationTypes;
import org.openmrs.module.mambaetl.helpers.reportOptions.TxNewAggregationTypes;
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
public class CXCATXDATIMReportsMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "f731add5-86b2-4af1-9a86-960262e1b06d";
	}
	
	@Override
	public String getName() {
		return "DATIM TREATMENT- CXCA_TX";
	}
	
	@Override
	public String getDescription() {
		return "CXCA_TX DATIM mamba report ";
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
		headerDefinition.setDescription("DSD: CXCA_TX");
		headerDefinition.setParameters(getParameters());
		reportDefinition.addDataSetDefinition("DSD: CXCA_TX", EthiOhriUtil.map(headerDefinition, "endDate=${endDateGC}"));
		
		CXCATXDataSetDefinitionMamba cxcaTxTotalDataSetDefinitionMamba = new CXCATXDataSetDefinitionMamba();
		cxcaTxTotalDataSetDefinitionMamba.addParameters(getParameters());
		cxcaTxTotalDataSetDefinitionMamba.setCxcatxAggregationTypes(CXCATXAggregationTypes.TOTAL);
		cxcaTxTotalDataSetDefinitionMamba
		        .setDescription("Number of women with a positive VIA screening test who are HIV-positive and on ART eligible for cryotherapy, thermocoagulation or LEEP who received cryotherapy, thermocoagulation or LEEP. Numerator will auto-calculate from the Age/Treatment Type/Screening Visit Type.");
		reportDefinition
		        .addDataSetDefinition(
		            "Number of women with a positive VIA screening test who are HIV-positive and on ART eligible for cryotherapy, thermocoagulation or LEEP who received cryotherapy, thermocoagulation or LEEP. Numerator will auto-calculate from the Age/Treatment Type/Screening Visit Type.",
		            EthiOhriUtil.map(cxcaTxTotalDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		CXCATXDataSetDefinitionMamba cxcatxFirstTimeDataSetDefinitionMamba = new CXCATXDataSetDefinitionMamba();
		cxcatxFirstTimeDataSetDefinitionMamba.addParameters(getParameters());
		cxcatxFirstTimeDataSetDefinitionMamba.setCxcatxAggregationTypes(CXCATXAggregationTypes.FIRST_TIME_SCREENING);
		cxcatxFirstTimeDataSetDefinitionMamba.setDescription("First time screened for cervical cancer");
		reportDefinition.addDataSetDefinition("First time screened for cervical cancer",
		    EthiOhriUtil.map(cxcatxFirstTimeDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		CXCATXDataSetDefinitionMamba cxcatxRescreenedDataSetDefinitionMamba = new CXCATXDataSetDefinitionMamba();
		cxcatxRescreenedDataSetDefinitionMamba.addParameters(getParameters());
		cxcatxRescreenedDataSetDefinitionMamba.setCxcatxAggregationTypes(CXCATXAggregationTypes.RE_SCREENING);
		cxcatxRescreenedDataSetDefinitionMamba.setDescription("Rescreened after previous negative or suspected cancer");
		reportDefinition.addDataSetDefinition("Rescreened after previous negative or suspected cancer",
		    EthiOhriUtil.map(cxcatxRescreenedDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		CXCATXDataSetDefinitionMamba cxcatxPostTreatmentDataSetDefinitionMamba = new CXCATXDataSetDefinitionMamba();
		cxcatxPostTreatmentDataSetDefinitionMamba.addParameters(getParameters());
		cxcatxPostTreatmentDataSetDefinitionMamba.setCxcatxAggregationTypes(CXCATXAggregationTypes.POST_TREATMENT);
		cxcatxPostTreatmentDataSetDefinitionMamba.setDescription("Post-treatment follow-up");
		reportDefinition.addDataSetDefinition("Post-treatment follow-up",
		    EthiOhriUtil.map(cxcatxPostTreatmentDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		
		ReportDesign design = ReportManagerUtil.createExcelDesign("c272f59e-4aa4-451d-ad7e-97add8caac65", reportDefinition);
		
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
