package org.openmrs.module.mambaetl.reports.datim.tx_curr;

import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_curr.CurrCoarseByAgeAndSexAndCD4DataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_curr.CurrFineByAgeAndSexAndCD4DataSetDefinitionMamba;
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
		return "d4debfa2-95ea-4c8e-9a89-36ccd201e943";
	} //4d7b385f-331f-400c-8592-f539f4565d9d
	
	@Override
	public String getName() {
		return "MAMBA DATIM- TX_CURR";
	}
	
	@Override
	public String getDescription() {
		return "TX curr DATIM mamba report ";
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
		
		CurrFineByAgeAndSexAndCD4DataSetDefinitionMamba lessThan200CD4DataSetDefinitionMambaCoarse = new CurrFineByAgeAndSexAndCD4DataSetDefinitionMamba();
		lessThan200CD4DataSetDefinitionMambaCoarse.addParameters(getParameters());
		lessThan200CD4DataSetDefinitionMambaCoarse.setCd4Status(Cd4Status.LOW);
		reportDefinition.addDataSetDefinition("List of Patients Currently on ART < 200 CD4",
		    map(lessThan200CD4DataSetDefinitionMambaCoarse, "endDate=${endDateGC}"));
		
		CurrFineByAgeAndSexAndCD4DataSetDefinitionMamba greaterThan200CD4DataSetDefinitionMambaCoarse = new CurrFineByAgeAndSexAndCD4DataSetDefinitionMamba();
		greaterThan200CD4DataSetDefinitionMambaCoarse.addParameters(getParameters());
		greaterThan200CD4DataSetDefinitionMambaCoarse.setCd4Status(Cd4Status.HIGH);
		reportDefinition.addDataSetDefinition("List of Patients Currently on ART > 200 CD4",
		    map(greaterThan200CD4DataSetDefinitionMambaCoarse, "endDate=${endDateGC}"));
		
		CurrFineByAgeAndSexAndCD4DataSetDefinitionMamba unknownCD4DataSetDefinitionMambaCoarse = new CurrFineByAgeAndSexAndCD4DataSetDefinitionMamba();
		unknownCD4DataSetDefinitionMambaCoarse.addParameters(getParameters());
		unknownCD4DataSetDefinitionMambaCoarse.setCd4Status(Cd4Status.UNKNOWN);
		reportDefinition.addDataSetDefinition("List of Patients Currently on ART Unknown CD4",
		    map(unknownCD4DataSetDefinitionMambaCoarse, "endDate=${endDateGC}"));
		
		CurrCoarseByAgeAndSexAndCD4DataSetDefinitionMamba lessThan200CD4DataSetDefinitionMambafine = new CurrCoarseByAgeAndSexAndCD4DataSetDefinitionMamba();
		lessThan200CD4DataSetDefinitionMambafine.addParameters(getParameters());
		lessThan200CD4DataSetDefinitionMambafine.setCd4Status(Cd4Status.LOW);
		reportDefinition.addDataSetDefinition("Fine List of Patients Currently on ART < 200 CD4",
		    map(lessThan200CD4DataSetDefinitionMambafine, "endDate=${endDateGC}"));
		
		CurrCoarseByAgeAndSexAndCD4DataSetDefinitionMamba greaterThan200CD4DataSetDefinitionMambafine = new CurrCoarseByAgeAndSexAndCD4DataSetDefinitionMamba();
		greaterThan200CD4DataSetDefinitionMambafine.addParameters(getParameters());
		greaterThan200CD4DataSetDefinitionMambafine.setCd4Status(Cd4Status.HIGH);
		reportDefinition.addDataSetDefinition("Fine List of Patients Currently on ART > 200 CD4",
		    map(greaterThan200CD4DataSetDefinitionMambafine, "endDate=${endDateGC}"));
		
		CurrCoarseByAgeAndSexAndCD4DataSetDefinitionMamba unknown200CD4DataSetDefinitionMambafine = new CurrCoarseByAgeAndSexAndCD4DataSetDefinitionMamba();
		unknown200CD4DataSetDefinitionMambafine.addParameters(getParameters());
		unknown200CD4DataSetDefinitionMambafine.setCd4Status(Cd4Status.UNKNOWN);
		reportDefinition.addDataSetDefinition("Fine List of Patients Currently on ART Unknown CD4",
		    map(unknown200CD4DataSetDefinitionMambafine, "endDate=${endDateGC}"));
		
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
		ReportDesign design = ReportManagerUtil.createExcelDesign("dd86d722-c9c9-4d23-beff-4e76ffbdc0b1", reportDefinition);
		
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
