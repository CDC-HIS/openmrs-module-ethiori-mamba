package org.openmrs.module.mambaetl.helpers.reportOptions;

public enum TxCurrAnalysisCategories {
	TX_CURR_THIS_MONTH("TX_CURR_THIS_MONTH"), TX_CURR_LAST_MONTH("TX_CURR_LAST_MONTH"), TX_CURR_NEWLY_INCLUDED(
	        "TX_CURR_NEWLY_INCLUDED"), TX_CURR_EXCLUDED_THIS_MONTH("TX_CURR_EXCLUDED_THIS_MONTH"), OTHER_OUTCOME(
	        "OTHER_OUTCOME"), NOT_UPDATED("NOT_UPDATED"), SUMMARY("SUMMARY"), ON_DSD("ON_DSD");
	
	private final String sqlValue;
	
	TxCurrAnalysisCategories(String sqlValue) {
		this.sqlValue = sqlValue;
	}
	
	public String getSqlValue() {
		return sqlValue;
	}
	
	public static TxCurrAnalysisCategories fromString(String status) {
		for (TxCurrAnalysisCategories cs : TxCurrAnalysisCategories.values()) {
			if (cs.name().equalsIgnoreCase(status) || cs.getSqlValue().equalsIgnoreCase(status)) {
				return cs;
			}
		}
		return SUMMARY;
	}
}
