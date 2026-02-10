package org.openmrs.module.mambaetl.api.dao;

import java.util.List;

public interface CustomMambaReportItemDao {

    /**
     * Gets a MambaReport by the report ID
     *
     * @param mambaReportId the MambaReport id
     * @return a list of MambaReport items with given id, or null if none exists
     */
    List<MambaReportItem> getMambaReport(String mambaReportId);

    /**
     * Gets all MambaReport results that match the given criteria
     *
     * @param criteria - the criteria for the returned MambaReport results
     * @return a list of all MambaReport items
     */
    List<MambaReportItem> getMambaReport(MambaReportCriteria criteria);

    /**
     * Get the Total records (size) of this report
     *
     * @param criteria to follow
     * @return total records
     */
    Integer getMambaReportSize(MambaReportCriteria criteria);
}