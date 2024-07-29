package org.openmrs.module.mambaetl.helpers.mapper;
import java.util.HashMap;
import java.util.Map;

public class DynamicData {

    private Map<String, Object> fields = new HashMap<>();

    public void setField(String fieldName, Object value) {
        fields.put(fieldName, value);
    }

    public Object getField(String fieldName) {
        return fields.get(fieldName);
    }

    @Override
    public String toString() {
        return "DynamicData{" +
                "fields=" + fields +
                '}';
    }
}
