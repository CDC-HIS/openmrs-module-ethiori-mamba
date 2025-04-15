package org.openmrs.module.mambaetl.helpers.mapper;

public enum AggregationType {
	CD4("cd4"), AGE_SEX("age_sex"), UNKNOWN("unknown");
	
	private final String sqlValue;
	
	AggregationType(String sqlValue) {
		this.sqlValue = sqlValue;
	}
	
	public String getSqlValue() {
		return sqlValue;
	}
	
	public static AggregationType fromString(String status) {
		for (AggregationType cs : AggregationType.values()) {
			if (cs.name().equalsIgnoreCase(status) || cs.getSqlValue().equalsIgnoreCase(status)) {
				return cs;
			}
		}
		return UNKNOWN;
	}
}
