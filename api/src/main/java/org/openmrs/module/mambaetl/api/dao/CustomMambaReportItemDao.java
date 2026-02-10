package org.openmrs.module.mambaetl.api.dao;

import org.openmrs.module.mambaetl.api.model.CustomMambaReportItem;
import java.util.List;

public interface CustomMambaReportItemDao {

    /**
     * Specific method to query the client table.
     */
    List<CustomMambaReportItem> getClientByUuid(String patientUuid);

    /**
     * Generic helper to execute any SQL query.
     */
    List<CustomMambaReportItem> executeSqlQuery(String sql, List<Object> params);
}