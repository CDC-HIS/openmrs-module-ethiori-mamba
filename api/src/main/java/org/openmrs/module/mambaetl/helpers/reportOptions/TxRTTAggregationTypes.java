package org.openmrs.module.mambaetl.helpers.reportOptions;

public enum TxRTTAggregationTypes {
	TOTAL("TOTAL"), CD4_LESS_THAN_200("CD4_LESS_THAN_200"), CD4_GREATER_THAN_200("CD4_GREATER_THAN_200"), CD4_UNKNOWN(
	        "CD4_UNKNOWN"), CD4_NOT_ELIGIBLE("CD4_NOT_ELIGIBLE"), IIT("IIT");
	
	private final String sqlValue;
	
	TxRTTAggregationTypes(String sqlValue) {
		this.sqlValue = sqlValue;
	}
	
	public String getSqlValue() {
		return sqlValue;
	}
	
	public static TxRTTAggregationTypes fromString(String status) {
		for (TxRTTAggregationTypes cs : TxRTTAggregationTypes.values()) {
			if (cs.name().equalsIgnoreCase(status) || cs.getSqlValue().equalsIgnoreCase(status)) {
				return cs;
			}
		}
		return CD4_LESS_THAN_200;
	}
}
