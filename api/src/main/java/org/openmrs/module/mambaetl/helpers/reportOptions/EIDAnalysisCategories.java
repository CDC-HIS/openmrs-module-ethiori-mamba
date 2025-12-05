package org.openmrs.module.mambaetl.helpers.reportOptions;

public enum EIDAnalysisCategories {
	DNA_PCR("DNA PCR"), RAPID_ANTIBODY("Rapid Antibody");
	
	private final String sqlValue;
	
	EIDAnalysisCategories(String sqlValue) {
		this.sqlValue = sqlValue;
	}
	
	public String getSqlValue() {
		return sqlValue;
	}
	
	public static EIDAnalysisCategories fromString(String status) {
		for (EIDAnalysisCategories cs : EIDAnalysisCategories.values()) {
			if (cs.name().equalsIgnoreCase(status) || cs.getSqlValue().equalsIgnoreCase(status)) {
				return cs;
			}
		}
		return DNA_PCR;
	}
}
