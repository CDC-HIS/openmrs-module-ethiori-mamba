package org.openmrs.module.mambaetl.api.impl;

import org.openmrs.api.impl.BaseOpenmrsService;
import org.openmrs.module.mambaetl.api.PatientSummaryService;
import org.openmrs.module.mambaetl.api.db.PatientSummaryDao;
import java.util.Map;

public class PatientSummaryServiceImpl extends BaseOpenmrsService implements PatientSummaryService {
	
	private PatientSummaryDao dao;
	
	public void setDao(PatientSummaryDao dao) {
		this.dao = dao;
	}
	
	@Override
	public Map<String, Object> getPatientSummary(String patientUuid) {
		return dao.getPatientSummary(patientUuid);
	}
}
