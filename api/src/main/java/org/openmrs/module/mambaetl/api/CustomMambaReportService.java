package org.openmrs.module.mambaetl.api;

/**
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 * <p>
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */

import org.openmrs.annotation.Authorized;
import org.openmrs.api.OpenmrsService;
import org.openmrs.module.mambaetl.api.criteria.CustomMambaReportCriteria;
import org.openmrs.module.mambaetl.api.model.CustomMambaReportItem;

import java.util.List;

/**
 * This interface defines an API of interacting with {@link CustomMambaReportItem} objects
 */
public interface CustomMambaReportService extends OpenmrsService {
	
	/**
	 * Gets MambaReport by id
	 * 
	 * @param mambaReportId the MambaReport id
	 * @return the MambaReport with given report id, or null if none exists
	 */
	@Authorized({ CustomMambaReportsConstants.VIEW_MAMBA_REPORT })
	List<CustomMambaReportItem> getMambaReport(String mambaReportId);
	
	/**
	 * Gets all MambaReport results that match the given criteria
	 * 
	 * @param criteria - the criteria for the returned MambaReport results
	 * @return a list of MambaReport
	 */
	@Authorized({ CustomMambaReportsConstants.VIEW_MAMBA_REPORT })
	List<CustomMambaReportItem> getMambaReportByCriteria(CustomMambaReportCriteria criteria);
	
	@Authorized({ CustomMambaReportsConstants.VIEW_MAMBA_REPORT })
	Integer getMambaReportSize(CustomMambaReportCriteria criteria);
}
