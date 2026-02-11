package org.openmrs.module.mambaetl.api;

import org.openmrs.api.OpenmrsService;
import org.springframework.transaction.annotation.Transactional;
import java.util.Map;

@Transactional
public interface PatientSummaryService extends OpenmrsService {
	
	@Transactional(readOnly = true)
	Map<String, Object> getPatientSummary(String patientUuid);
}
