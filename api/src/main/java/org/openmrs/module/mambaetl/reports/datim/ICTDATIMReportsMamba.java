package org.openmrs.module.mambaetl.reports.datim;

import org.openmrs.module.mambaetl.datasetdefinition.datim.HeaderDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.KeyPopulationDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.hts_index.HTSIndexDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_curr.TxCurrAgeSexDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.mambaetl.helpers.reportOptions.HTSIndexAggregationTypes;
import org.openmrs.module.mambaetl.helpers.reportOptions.TxCurrAggregationTypes;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.ReportRequest;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.reporting.report.manager.ReportManager;
import org.openmrs.module.reporting.report.manager.ReportManagerUtil;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Component
public class ICTDATIMReportsMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "91b8dac5-cbc7-4f07-b402-0cad9a29b605";
	}
	
	@Override
	public String getName() {
		return "DATIM TESTING- HTS_INDEX";
	}
	
	@Override
	public String getDescription() {
		return "HTS_INDEX DATIM mamba report ";
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
		headerDefinition.setDescription("DSD: HTS_INDEX");
		headerDefinition.setParameters(getParameters());
		reportDefinition.addDataSetDefinition("DSD: HTS_INDEX", EthiOhriUtil.map(headerDefinition, "endDate=${endDateGC}"));
		
		HTSIndexDataSetDefinitionMamba htsIndexTotalDataSetDefinitionMamba = new HTSIndexDataSetDefinitionMamba();
		htsIndexTotalDataSetDefinitionMamba.addParameters(getParameters());
		htsIndexTotalDataSetDefinitionMamba.setHtsIndexAggregationTypes(HTSIndexAggregationTypes.ICT_TOTAL);
		htsIndexTotalDataSetDefinitionMamba
		        .setDescription("Number of individuals who were identified and tested using index testing services and received their results.");
		reportDefinition.addDataSetDefinition(
		    "Number of individuals who were identified and tested using index testing services and received their results.",
		    EthiOhriUtil.map(htsIndexTotalDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		HTSIndexDataSetDefinitionMamba htsIndexOfferedDataSetDefinitionMamba = new HTSIndexDataSetDefinitionMamba();
		htsIndexOfferedDataSetDefinitionMamba.addParameters(getParameters());
		htsIndexOfferedDataSetDefinitionMamba.setHtsIndexAggregationTypes(HTSIndexAggregationTypes.TESTING_OFFERED);
		htsIndexOfferedDataSetDefinitionMamba
		        .setDescription("Number of index cases offered index testing services by age/sex");
		reportDefinition.addDataSetDefinition("Number of index cases offered index testing services by age/sex",
		    EthiOhriUtil.map(htsIndexOfferedDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		HTSIndexDataSetDefinitionMamba htsIndexAcceptedDataSetDefinitionMamba = new HTSIndexDataSetDefinitionMamba();
		htsIndexAcceptedDataSetDefinitionMamba.addParameters(getParameters());
		htsIndexAcceptedDataSetDefinitionMamba.setHtsIndexAggregationTypes(HTSIndexAggregationTypes.TESTING_ACCEPTED);
		htsIndexAcceptedDataSetDefinitionMamba
		        .setDescription("Number of index cases that accepted index testing services by age/sex");
		reportDefinition.addDataSetDefinition("Number of index cases that accepted index testing services by age/sex",
		    EthiOhriUtil.map(htsIndexAcceptedDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		HTSIndexDataSetDefinitionMamba htsIndexEllicitedDataSetDefinitionMamba = new HTSIndexDataSetDefinitionMamba();
		htsIndexEllicitedDataSetDefinitionMamba.addParameters(getParameters());
		htsIndexEllicitedDataSetDefinitionMamba.setHtsIndexAggregationTypes(HTSIndexAggregationTypes.ELICITED);
		htsIndexEllicitedDataSetDefinitionMamba.setDescription("Number of contacts elicited by age/sex");
		reportDefinition.addDataSetDefinition("Number of contacts elicited by age/sex",
		    EthiOhriUtil.map(htsIndexEllicitedDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		HTSIndexDataSetDefinitionMamba htsIndexKnownPositivesDataSetDefinitionMamba = new HTSIndexDataSetDefinitionMamba();
		htsIndexKnownPositivesDataSetDefinitionMamba.addParameters(getParameters());
		htsIndexKnownPositivesDataSetDefinitionMamba.setHtsIndexAggregationTypes(HTSIndexAggregationTypes.KNOWN_POSITIVE);
		htsIndexKnownPositivesDataSetDefinitionMamba
		        .setDescription("Number of contacts tested by test result and age/sex, Known Positives:");
		reportDefinition.addDataSetDefinition("Number of contacts tested by test result and age/sex, Known Positives:",
		    EthiOhriUtil.map(htsIndexEllicitedDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		HTSIndexDataSetDefinitionMamba htsIndexNewlyTestedPositivesDataSetDefinitionMamba = new HTSIndexDataSetDefinitionMamba();
		htsIndexNewlyTestedPositivesDataSetDefinitionMamba.addParameters(getParameters());
		htsIndexNewlyTestedPositivesDataSetDefinitionMamba
		        .setHtsIndexAggregationTypes(HTSIndexAggregationTypes.NEW_POSITIVE);
		htsIndexNewlyTestedPositivesDataSetDefinitionMamba
		        .setDescription("Number of contacts tested by test result and age/sex, Newly Tested Positives :");
		reportDefinition.addDataSetDefinition(
		    "Number of contacts tested by test result and age/sex, Newly Tested Positives :", EthiOhriUtil.map(
		        htsIndexNewlyTestedPositivesDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));
		
		HTSIndexDataSetDefinitionMamba htsIndexNewlyTestedNegativesDataSetDefinitionMamba = new HTSIndexDataSetDefinitionMamba();
		htsIndexNewlyTestedNegativesDataSetDefinitionMamba.addParameters(getParameters());
		htsIndexNewlyTestedNegativesDataSetDefinitionMamba
		        .setHtsIndexAggregationTypes(HTSIndexAggregationTypes.NEW_NEGATIVE);
		htsIndexNewlyTestedNegativesDataSetDefinitionMamba
		        .setDescription("Number of contacts tested by test result and age/sex, New Negatives :");
		reportDefinition.addDataSetDefinition("Number of contacts tested by test result and age/sex, New Negatives :",
		    EthiOhriUtil.map(htsIndexNewlyTestedNegativesDataSetDefinitionMamba,
		        "startDate=${startDateGC},endDate=${endDateGC}"));
		
		HTSIndexDataSetDefinitionMamba htsIndexDocumentedNegativesDataSetDefinitionMamba = new HTSIndexDataSetDefinitionMamba();
		htsIndexDocumentedNegativesDataSetDefinitionMamba.addParameters(getParameters());
		htsIndexDocumentedNegativesDataSetDefinitionMamba.setHtsIndexAggregationTypes(HTSIndexAggregationTypes.NEW_NEGATIVE);
		htsIndexDocumentedNegativesDataSetDefinitionMamba
		        .setDescription("Number of contacts tested by test result and age/sex, Documented Negatives");
		reportDefinition.addDataSetDefinition("Number of contacts tested by test result and age/sex, Documented Negatives",
		    EthiOhriUtil.map(htsIndexDocumentedNegativesDataSetDefinitionMamba,
		        "startDate=${startDateGC},endDate=${endDateGC}"));
		
		return reportDefinition;
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		ReportDesign design = ReportManagerUtil.createExcelDesign("cd44add3-9c5e-460c-a975-9fa7ac2865c7", reportDefinition);
		
		return Collections.singletonList(design);
	}
	
	@Override
	public List<ReportRequest> constructScheduledRequests(ReportDefinition reportDefinition) {
		return new ArrayList<>();
	}
	
	@Override
	public String getVersion() {
		return "1.0.0-SNAPSHOT";
	}
}
