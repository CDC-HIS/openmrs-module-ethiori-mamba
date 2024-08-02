package org.openmrs.module.mambaetl.reports.datim.tx_curr;

import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_curr.CurrFineByAgeAndSexAndCD4DataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_new.CoarseByAgeAndSexAndCD4DataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_new.FineByAgeAndSexAndCD4DataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.linelist.TxCurrDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.mambaetl.helpers.mapper.Cd4Status;
import org.openmrs.module.reporting.evaluation.parameter.Mapped;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.evaluation.parameter.Parameterizable;
import org.openmrs.module.reporting.evaluation.parameter.ParameterizableUtil;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.ReportRequest;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.reporting.report.manager.ReportManager;
import org.openmrs.module.reporting.report.manager.ReportManagerUtil;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class TxCurrDATIMReportsMamba implements ReportManager {
	
	@Override
	public String getUuid() {
		return "38646880-45ad-4927-a1a1-3aef8627cb77";
	} //4d7b385f-331f-400c-8592-f539f4565d9d
	
	@Override
	public String getName() {
		return "DATIM- TX_CURR_CD4";
	}
	
	@Override
	public String getDescription() {
		return "TX curr CD4 DATIM mamba report ";
	}
	
	@Override
	public List<Parameter> getParameters() {
		
		Parameter endDate = new Parameter("endDate", "On Month", Date.class);
		endDate.setRequired(false);
		Parameter endDateGC = new Parameter("endDateGC", " ", Date.class);
		endDateGC.setRequired(false);
		return Arrays.asList(endDate, endDateGC);
		
	}
	
	@Override
	public ReportDefinition constructReportDefinition() {
		ReportDefinition reportDefinition = new ReportDefinition();
		reportDefinition.setUuid(getUuid());
		reportDefinition.setName(getName());
		reportDefinition.setDescription(getDescription());
		
		reportDefinition.setParameters(getParameters());
		
		CurrFineByAgeAndSexAndCD4DataSetDefinitionMamba currFineByAgeAndSexAndCD4DataSetDefinitionMamba = new CurrFineByAgeAndSexAndCD4DataSetDefinitionMamba();
		currFineByAgeAndSexAndCD4DataSetDefinitionMamba.addParameters(getParameters());
		
		reportDefinition.addDataSetDefinition("List of Patients Currently on ART",
		    map(currFineByAgeAndSexAndCD4DataSetDefinitionMamba, "endDate=${endDateGC}"));
		return reportDefinition;
	}
	
	public static <T extends Parameterizable> Mapped<T> map(T parameterizable, String mappings) {
		if (parameterizable == null) {
			throw new IllegalArgumentException("Parameterizable cannot be null");
		}
		if (mappings == null) {
			mappings = ""; // probably not necessary, just to be safe
		}
		return new Mapped<>(parameterizable, ParameterizableUtil.createParameterMappings(mappings));
	}
	
	@Override
	public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
		ReportDesign design = ReportManagerUtil.createExcelDesign("d5681725-e4a4-4234-ab07-d85595da8ff2", reportDefinition);
		
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
