package org.openmrs.module.mambaetl.helpers.reportOptions;

public enum HEIAggregationTypes {
	TOTAL("TOTAL"), RESULT_COLLECTED("RESULT_COLLECTED"), PREP_TYPE("PREP_TYPE"), PREGNANT_BF("PREGNANT_BF"), FACILITY(
	        "FACILITY");
	
	private final String sqlValue;
	
	HEIAggregationTypes(String sqlValue) {
		this.sqlValue = sqlValue;
	}
	
	public String getSqlValue() {
		return sqlValue;
	}
	
	public static HEIAggregationTypes fromString(String status) {
		for (HEIAggregationTypes cs : HEIAggregationTypes.values()) {
			if (cs.name().equalsIgnoreCase(status) || cs.getSqlValue().equalsIgnoreCase(status)) {
				return cs;
			}
		}
		return RESULT_COLLECTED;
	}
}
