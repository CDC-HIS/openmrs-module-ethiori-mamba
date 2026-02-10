package org.openmrs.module.mambaetl.api.criteria;

import java.util.ArrayList;
import java.util.List;

public class CustomMambaReportCriteria {

    private String reportId;
    private Integer pageNumber = 1;
    private Integer pageSize = 100;
    private List<CustomMambaReportSearchField> searchFields = new ArrayList<>();

    public CustomMambaReportCriteria() {
    }

    public CustomMambaReportCriteria(String reportId) {
        this.reportId = reportId;
    }


    public void addParameter(String name, String value) {
        if (this.searchFields == null) {
            this.searchFields = new ArrayList<>();
        }
        this.searchFields.add(new CustomMambaReportSearchField(name, value));
    }

    public String getReportId() {
        return reportId;
    }

    public void setReportId(String reportId) {
        this.reportId = reportId;
    }

    public Integer getPageNumber() {
        return pageNumber;
    }

    public void setPageNumber(Integer pageNumber) {
        this.pageNumber = pageNumber;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    public List<CustomMambaReportSearchField> getSearchFields() {
        return searchFields;
    }

    public void setSearchFields(List<CustomMambaReportSearchField> searchFields) {
        this.searchFields = searchFields;
    }
}