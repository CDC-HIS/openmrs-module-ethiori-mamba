package org.openmrs.module.mambaetl.api.model;

import java.util.ArrayList;
import java.util.List;

public class CustomMambaReportItem {

    private Integer serialId;
    private List<CustomMambaReportItemColumn> record = new ArrayList<>();

    public Integer getSerialId() {
        return serialId;
    }

    public void setSerialId(Integer serialId) {
        this.serialId = serialId;
    }

    public List<CustomMambaReportItemColumn> getRecord() {
        return record;
    }

    public void setRecord(List<CustomMambaReportItemColumn> record) {
        this.record = record;
    }
}