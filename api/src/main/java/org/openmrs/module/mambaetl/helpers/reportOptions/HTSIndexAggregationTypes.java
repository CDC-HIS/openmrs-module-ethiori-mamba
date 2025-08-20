package org.openmrs.module.mambaetl.helpers.reportOptions;

public enum HTSIndexAggregationTypes {
	TESTING_OFFERED("TESTING_OFFERED"), TESTING_ACCEPTED("TESTING_ACCEPTED"), ELICITED("ELICITED"), KNOWN_POSITIVE("KNOWN_POSITIVE"), DOCUMENTED_NEGATIVE("DOCUMENTED_NEGATIVE"), NEW_POSITIVE(
	        "NEW_POSITIVE"),NEW_NEGATIVE("NEW_NEGATIVE"),ICT_TOTAL("ICT_TOTAL");

	private final String sqlValue;

	HTSIndexAggregationTypes(String sqlValue) {
		this.sqlValue = sqlValue;
	}
	
	public String getSqlValue() {
		return sqlValue;
	}
	
	public static HTSIndexAggregationTypes fromString(String status) {
		for (HTSIndexAggregationTypes cs : HTSIndexAggregationTypes.values()) {
			if (cs.name().equalsIgnoreCase(status) || cs.getSqlValue().equalsIgnoreCase(status)) {
				return cs;
			}
		}
		return ICT_TOTAL;
	}
}
