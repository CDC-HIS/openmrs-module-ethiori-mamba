package org.openmrs.module.mambaetl.helpers.reportOptions;

public enum TxCurrAnalysisCategories {
	TX_CURR_THIS_MONTH("This Month TX_Curr"), TX_CURR_LAST_MONTH("Previous Month Tx_Curr"), TX_CURR_NEWLY_INCLUDED(
	        "Newly Included Tx_Curr"), TX_CURR_EXCLUDED_THIS_MONTH("Excluded From Tx_Curr"), OTHER_OUTCOME(
	        "Other Outcome"), NOT_UPDATED("Not Updated"), SUMMARY("Tx_Curr Summary"), ON_DSD("Tx_Curr On DSD");
	
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
