package org.openmrs.module.mambaetl.helpers.reportOptions;

public enum PrEPCTggregationTypes {
	TOTAL("TOTAL"), AGE_SEX("AGE_SEX"), TEST_RESULT("TEST_RESULT"), PREP_TYPE("PREP_TYPE"), PREGNANT_BF("PREGNANT_BF"), FACILITY(
	        "FACILITY");
	
	private final String sqlValue;
	
	PrEPCTggregationTypes(String sqlValue) {
		this.sqlValue = sqlValue;
	}
	
	public String getSqlValue() {
		return sqlValue;
	}
	
	public static PrEPCTggregationTypes fromString(String status) {
		for (PrEPCTggregationTypes cs : PrEPCTggregationTypes.values()) {
			if (cs.name().equalsIgnoreCase(status) || cs.getSqlValue().equalsIgnoreCase(status)) {
				return cs;
			}
		}
		return AGE_SEX;
	}
}
