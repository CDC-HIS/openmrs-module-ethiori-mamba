package org.openmrs.module.mambaetl.helpers.reportOptions;

public enum TxCurrAnalysisCategories {
	SUMMARY("Tx_Curr Summary"), TX_CURR_LAST_MONTH("Previous Month Tx_Curr"), TX_CURR_EXCLUDED_THIS_MONTH("Excluded From Tx_Curr"), NOT_UPDATED(
			"Not Updated"), TX_CURR_NEWLY_INCLUDED("Newly Included Tx_Curr"),TX_CURR_THIS_MONTH("This Month TX_Curr"),  ON_DSD("Tx_Curr On DSD"), OTHER_OUTCOME("Other Outcome");
	
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
