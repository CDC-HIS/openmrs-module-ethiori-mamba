package org.openmrs.module.mambaetl.helpers.reportOptions;

public enum TxCurrPvlsAggregationTypes {
	NUMERATOR_TOTAL("NUMERATOR_TOTAL"), NUMERATOR("NUMERATOR"), NUMERATOR_BREAST_FEEDING_PREGNANT(
	        "NUMERATOR_BREAST_FEEDING_PREGNANT"), DENOMINATOR_TOTAL("DENOMINATOR_TOTAL"), DENOMINATOR("DENOMINATOR"), DENOMINATOR_BREAST_FEEDING_PREGNANT(
	        "DENOMINATOR_BREAST_FEEDING_PREGNANT"), UNKNOWN("UNKNOWN");
	
	private final String sqlValue;
	
	TxCurrPvlsAggregationTypes(String sqlValue) {
		this.sqlValue = sqlValue;
	}
	
	public String getSqlValue() {
		return sqlValue;
	}
	
	public static TxCurrPvlsAggregationTypes fromString(String status) {
		for (TxCurrPvlsAggregationTypes cs : TxCurrPvlsAggregationTypes.values()) {
			if (cs.name().equalsIgnoreCase(status) || cs.getSqlValue().equalsIgnoreCase(status)) {
				return cs;
			}
		}
		return UNKNOWN;
	}
	
}
