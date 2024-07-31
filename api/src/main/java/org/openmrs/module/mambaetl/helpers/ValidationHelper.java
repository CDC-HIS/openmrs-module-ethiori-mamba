package org.openmrs.module.mambaetl.helpers;

import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_new.CoarseByAgeAndSexAndCD4DataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_new.FineByAgeAndSexAndCD4DataSetDefinitionMamba;
import org.openmrs.module.reporting.dataset.DataSetColumn;
import org.openmrs.module.reporting.dataset.DataSetRow;
import org.openmrs.module.reporting.dataset.SimpleDataSet;
import org.openmrs.module.reporting.evaluation.EvaluationException;

import java.sql.Date;

public class ValidationHelper {
	
	public <T> void validateDates(T definition, SimpleDataSet data) throws EvaluationException {
		Date startDate = null;
		Date endDate = null;
		if (definition instanceof FineByAgeAndSexAndCD4DataSetDefinitionMamba) {
			FineByAgeAndSexAndCD4DataSetDefinitionMamba def = (FineByAgeAndSexAndCD4DataSetDefinitionMamba) definition;
			startDate = new java.sql.Date(def.getStartDate().getTime());
			endDate = new java.sql.Date(def.getEndDate().getTime());
		}
		if (definition instanceof CoarseByAgeAndSexAndCD4DataSetDefinitionMamba) {
			CoarseByAgeAndSexAndCD4DataSetDefinitionMamba def = (CoarseByAgeAndSexAndCD4DataSetDefinitionMamba) definition;
			startDate = new java.sql.Date(def.getStartDate().getTime());
			endDate = new java.sql.Date(def.getEndDate().getTime());
			
		}
		ValidateDates(data, startDate, endDate);
		
	}
	
	static void ValidateDates(SimpleDataSet data, java.util.Date startDate, java.util.Date endDate)
	        throws EvaluationException {
		if (startDate != null && endDate != null && startDate.after(endDate)) {
			DataSetRow row = new DataSetRow();
			row.addColumnValue(new DataSetColumn("Error", "Error", String.class),
			    "Report start date cannot be after report end date");
			data.addRow(row);
			throw new EvaluationException("Start date cannot be greater than end date");
		}
	}
}
