package org.openmrs.module.mambaetl.helpers.mapper;

import java.util.HashMap;
import java.util.Map;
import org.openmrs.module.reporting.dataset.DataSetColumn;

public class DynamicDataSet {

    private final Map<String, DataSetColumn> columnMap = new HashMap<>();

    public void addColumn(DataSetColumn column) {
        columnMap.put(column.getName(), column);
    }

    public DataSetColumn getColumn(String columnName) {
        return columnMap.get(columnName);
    }

}
