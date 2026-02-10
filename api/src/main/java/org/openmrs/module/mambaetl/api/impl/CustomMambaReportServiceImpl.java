package org.openmrs.module.mambaetl.api.impl;

import org.openmrs.api.impl.BaseOpenmrsService;
import org.openmrs.module.mambaetl.api.CustomMambaReportService;
import org.openmrs.module.mambaetl.api.dao.CustomMambaReportItemDao;
import org.openmrs.module.mambaetl.api.model.CustomMambaReportItem;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Transactional
public class CustomMambaReportServiceImpl extends BaseOpenmrsService implements CustomMambaReportService {

	private CustomMambaReportItemDao dao;

	public void setDao(CustomMambaReportItemDao dao) {
		this.dao = dao;
	}

	// This is now your primary method for this specific requirement
	public List<CustomMambaReportItem> getClientByUuid(String patientUuid) {
		return dao.getClientByUuid(patientUuid);
	}
}