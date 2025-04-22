package org.openmrs.module.mambaetl.helpers.reportOptions;

public enum TxNewAggregationTypes {
	HIGH(">=200"), LOW("<200"), UNKNOWN("unknown"), NOT_TESTED("not_tested"), NUMERATOR("numerator"), BREAST_FEEDING(
	        "breast_feeding");
	
	private final String sqlValue;
	
	TxNewAggregationTypes(String sqlValue) {
		this.sqlValue = sqlValue;
	}
	
	public String getSqlValue() {
		return sqlValue;
	}
	
	public static TxNewAggregationTypes fromString(String status) {
		for (TxNewAggregationTypes cs : TxNewAggregationTypes.values()) {
			if (cs.name().equalsIgnoreCase(status) || cs.getSqlValue().equalsIgnoreCase(status)) {
				return cs;
			}
		}
		return UNKNOWN;
	}
}
