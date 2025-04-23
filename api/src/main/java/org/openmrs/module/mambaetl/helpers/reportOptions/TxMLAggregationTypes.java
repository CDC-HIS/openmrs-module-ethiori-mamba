package org.openmrs.module.mambaetl.helpers.reportOptions;

public enum TxMLAggregationTypes {
	DIED("DIED"), LESS_THAN_THREE_MONTHS("LESS_THAN_THREE_MONTHS"), THREE_TO_FIVE_MONTHS("THREE_TO_FIVE_MONTHS"), MORE_THAN_SIX_MONTHS(
	        "MORE_THAN_SIX_MONTHS"), TRANSFERRED_OUT("TRANSFERRED_OUT"), REFUSED("REFUSED");
	
	private final String sqlValue;
	
	TxMLAggregationTypes(String sqlValue) {
		this.sqlValue = sqlValue;
	}
	
	public String getSqlValue() {
		return sqlValue;
	}
	
	public static TxMLAggregationTypes fromString(String status) {
		for (TxMLAggregationTypes cs : TxMLAggregationTypes.values()) {
			if (cs.name().equalsIgnoreCase(status) || cs.getSqlValue().equalsIgnoreCase(status)) {
				return cs;
			}
		}
		return DIED;
	}
}
