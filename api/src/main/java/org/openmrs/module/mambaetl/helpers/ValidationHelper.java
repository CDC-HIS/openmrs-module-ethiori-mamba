package org.openmrs.module.mambaetl.helpers;

import org.openmrs.module.reporting.dataset.SimpleDataSet;
import org.openmrs.module.reporting.evaluation.EvaluationException;

public class ValidationHelper {
	
	public static void ValidateDates(SimpleDataSet data, java.util.Date startDate, java.util.Date endDate)
	        throws EvaluationException {
		if (startDate != null && endDate != null && startDate.after(endDate)) {
			throw new EvaluationException("Start date cannot be greater than end date");
		}
	}
}
