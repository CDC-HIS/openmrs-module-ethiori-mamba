package org.openmrs.module.mambaetl.helpers.reportOptions;

public enum TBPrevAggregationTypes {
	TOTAL("TOTAL"), NEW_ART("NEW_ART"), PREV_ART("PREV_ART"), DEBUG("DEBUG");
	
	private final String sqlValue;
	
	TBPrevAggregationTypes(String sqlValue) {
		this.sqlValue = sqlValue;
	}
	
	public String getSqlValue() {
		return sqlValue;
	}
	
	public static TBPrevAggregationTypes fromString(String status) {
		for (TBPrevAggregationTypes cs : TBPrevAggregationTypes.values()) {
			if (cs.name().equalsIgnoreCase(status) || cs.getSqlValue().equalsIgnoreCase(status)) {
				return cs;
			}
		}
		return TOTAL;
	}
}
