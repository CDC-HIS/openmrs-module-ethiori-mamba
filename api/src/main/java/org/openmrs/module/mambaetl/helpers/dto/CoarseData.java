package org.openmrs.module.mambaetl.helpers.dto;

public class CoarseData {
	
	private final String sex;
	
	private final String age_15_plus;
	
	private final String age_15_minus;
	
	public CoarseData(String sex, String age_15_plus, String age_15_minus) {
		this.sex = sex;
		this.age_15_plus = age_15_plus;
		this.age_15_minus = age_15_minus;
	}
	
	public String getSex() {
		return sex;
	}
	
	public String getAge_15_plus() {
		return age_15_plus;
	}
	
	public String getAge_15_minus() {
		return age_15_minus;
	}
}
