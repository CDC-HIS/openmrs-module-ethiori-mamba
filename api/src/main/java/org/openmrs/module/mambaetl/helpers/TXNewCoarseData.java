package org.openmrs.module.mambaetl.helpers;

public class TXNewCoarseData {
	
	private final String sex;
	
	private final String fifteen_plus;
	
	private final String fifteen_minus;
	
	public TXNewCoarseData(String sex, String fifteen_plus, String fifteen_minus) {
		this.sex = sex;
		this.fifteen_plus = fifteen_plus;
		this.fifteen_minus = fifteen_minus;
	}
	
	public String getSex() {
		return sex;
	}
	
	public String getFifteen_plus() {
		return fifteen_plus;
	}
	
	public String getFifteen_minus() {
		return fifteen_minus;
	}
}
