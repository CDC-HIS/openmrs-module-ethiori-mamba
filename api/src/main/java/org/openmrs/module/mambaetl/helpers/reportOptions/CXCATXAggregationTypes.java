package org.openmrs.module.mambaetl.helpers.reportOptions;

public enum CXCATXAggregationTypes {
	TOTAL("TOTAL"), FIRST_TIME_SCREENING("FIRST_TIME_SCREENING"), RE_SCREENING("RE_SCREENING"), POST_TREATMENT(
	        "POST_TREATMENT");
	
	private final String sqlValue;
	
	CXCATXAggregationTypes(String sqlValue) {
		this.sqlValue = sqlValue;
	}
	
	public String getSqlValue() {
		return sqlValue;
	}
	
	public static CXCATXAggregationTypes fromString(String status) {
		for (CXCATXAggregationTypes cs : CXCATXAggregationTypes.values()) {
			if (cs.name().equalsIgnoreCase(status) || cs.getSqlValue().equalsIgnoreCase(status)) {
				return cs;
			}
		}
		return FIRST_TIME_SCREENING;
	}
}
