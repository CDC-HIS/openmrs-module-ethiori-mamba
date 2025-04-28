package org.openmrs.module.mambaetl.helpers.reportOptions;

public enum TxTBAggregationTypes {
	NEW_ART_POSITIVE("NEW_ART_POSITIVE"), NEW_ART_NEGATIVE("NEW_ART_NEGATIVE"), PREV_ART_POSITIVE("PREV_ART_POSITIVE"), PREV_ART_NEGATIVE(
	        "PREV_ART_NEGATIVE"), SCREEN_TYPE("SCREEN_TYPE"), SPECIMEN_SENT("SPECIMEN_SENT"), DIAGNOSTIC_TEST(
	        "DIAGNOSTIC_TEST"), POSITIVE_RESULT("POSITIVE_RESULT"), NUMERATOR_NEW("NUMERATOR_NEW"), NUMERATOR_PREV(
	        "NUMERATOR_PREV");
	
	private final String sqlValue;
	
	TxTBAggregationTypes(String sqlValue) {
		this.sqlValue = sqlValue;
	}
	
	public String getSqlValue() {
		return sqlValue;
	}
	
	public static TxTBAggregationTypes fromString(String status) {
		for (TxTBAggregationTypes cs : TxTBAggregationTypes.values()) {
			if (cs.name().equalsIgnoreCase(status) || cs.getSqlValue().equalsIgnoreCase(status)) {
				return cs;
			}
		}
		return NEW_ART_POSITIVE;
	}
}
