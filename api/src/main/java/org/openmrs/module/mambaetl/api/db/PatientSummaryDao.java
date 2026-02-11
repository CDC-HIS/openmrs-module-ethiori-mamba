package org.openmrs.module.mambaetl.api.db;

import java.util.Map;

public interface PatientSummaryDao {
	
	Map<String, Object> getPatientSummary(String patientUuid);
}
