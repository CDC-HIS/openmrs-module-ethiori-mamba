package org.openmrs.module.mambaetl.helpers.reportOptions;

public enum TxCurrAggregationTypes {
	CD4("cd4"), AGE_SEX("age_sex"), NUMERATOR("NUMERATOR");
	
	private final String sqlValue;
	
	TxCurrAggregationTypes(String sqlValue) {
		this.sqlValue = sqlValue;
	}
	
	public String getSqlValue() {
		return sqlValue;
	}
	
	public static TxCurrAggregationTypes fromString(String status) {
		for (TxCurrAggregationTypes cs : TxCurrAggregationTypes.values()) {
			if (cs.name().equalsIgnoreCase(status) || cs.getSqlValue().equalsIgnoreCase(status)) {
				return cs;
			}
		}
		return AGE_SEX;
	}
}
