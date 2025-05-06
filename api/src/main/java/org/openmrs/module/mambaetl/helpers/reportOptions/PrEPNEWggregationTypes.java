package org.openmrs.module.mambaetl.helpers.reportOptions;

public enum PrEPNEWggregationTypes {
	TOTAL("TOTAL"), AGE_SEX("AGE_SEX"), PREP_TYPE("PREP_TYPE"), PREGNANT_BF("PREGNANT_BF"), FACILITY("FACILITY");
	
	private final String sqlValue;
	
	PrEPNEWggregationTypes(String sqlValue) {
		this.sqlValue = sqlValue;
	}
	
	public String getSqlValue() {
		return sqlValue;
	}
	
	public static PrEPNEWggregationTypes fromString(String status) {
		for (PrEPNEWggregationTypes cs : PrEPNEWggregationTypes.values()) {
			if (cs.name().equalsIgnoreCase(status) || cs.getSqlValue().equalsIgnoreCase(status)) {
				return cs;
			}
		}
		return AGE_SEX;
	}
}
