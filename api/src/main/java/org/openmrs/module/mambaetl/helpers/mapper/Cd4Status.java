package org.openmrs.module.mambaetl.helpers.mapper;

public enum Cd4Status {
	HIGH(">=200"), LOW("<200"), UNKNOWN("unknown"), NOT_TESTED("not_tested"), NUMERATOR("numerator"), BREAST_FEEDING(
	        "breast_feeding");
	
	private final String sqlValue;
	
	Cd4Status(String sqlValue) {
		this.sqlValue = sqlValue;
	}
	
	public String getSqlValue() {
		return sqlValue;
	}
	
	public static Cd4Status fromString(String status) {
		for (Cd4Status cs : Cd4Status.values()) {
			if (cs.name().equalsIgnoreCase(status) || cs.getSqlValue().equalsIgnoreCase(status)) {
				return cs;
			}
		}
		return UNKNOWN;
	}
}
