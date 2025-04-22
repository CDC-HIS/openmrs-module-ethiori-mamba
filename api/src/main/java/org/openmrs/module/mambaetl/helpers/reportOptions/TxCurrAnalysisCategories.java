package org.openmrs.module.mambaetl.helpers.reportOptions;

public enum TxCurrAnalysisCategories {
	TRACED_BACK("TRACED_BACK"), NEW_ON_ART("NEW_ON_ART"), RESTARTED("RESTARTED"), TRANSFERRED_OUT("TRANSFERRED_OUT"), UNKNOWN(
	        "UNKNOWN");
	
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
		return UNKNOWN;
	}
}
