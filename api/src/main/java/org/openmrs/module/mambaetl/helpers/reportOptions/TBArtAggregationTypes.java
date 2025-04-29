package org.openmrs.module.mambaetl.helpers.reportOptions;

public enum TBArtAggregationTypes {
	ALREADY_ON_ART("ALREADY_ON_ART"), NEW_ON_ART("NEW_ON_ART");
	
	private final String sqlValue;
	
	TBArtAggregationTypes(String sqlValue) {
		this.sqlValue = sqlValue;
	}
	
	public String getSqlValue() {
		return sqlValue;
	}
	
	public static TBArtAggregationTypes fromString(String status) {
		for (TBArtAggregationTypes cs : TBArtAggregationTypes.values()) {
			if (cs.name().equalsIgnoreCase(status) || cs.getSqlValue().equalsIgnoreCase(status)) {
				return cs;
			}
		}
		return ALREADY_ON_ART;
	}
}
