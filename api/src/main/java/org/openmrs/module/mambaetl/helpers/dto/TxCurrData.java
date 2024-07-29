package org.openmrs.module.mambaetl.helpers.dto;

import java.util.Date;

public class TxCurrData {
	
	private final String patientName;
	
	private final String mrn;
	
	private final String uan;
	
	private final int currentAge;
	
	private final int ageAtEnrollment;
	
	private final String sex;
	
	private final String mobileNumber;
	
	private final Date hivConfirmedDate;
	
	private final Date artStartDate;
	
	private final Date followupDate;
	
	private final int weightInKg;
	
	private final String pregnancyStatus;
	
	private final String regimen;
	
	private final String arvDoseDays;
	
	private final String followUpStatus;
	
	private final String anitiretroviralAdherenceLevel;
	
	private final Date nextVisitDate;
	
	private final String dsdCategory;
	
	private final Date tptStartDate;
	
	private final Date tptCompletedDate;
	
	private final Date tptDiscontinuedDate;
	
	private final Date tuberculosisTreatmentEndDate;
	
	private final Date dateViralLoadResultsReceived;
	
	private final String viralLoadTestStatus;
	
	private final Date VLEligibilityDate;
	
	public TxCurrData(String patientName, String mrn, String uan, int currentAge, int ageAtEnrollment, String sex,
	    String mobileNumber, Date hivConfirmedDate, Date artStartDate, Date followupDate, int weightInKg,
	    String pregnancyStatus, String regimen, String arvDoseDays, String followUpStatus,
	    String anitiretroviralAdherenceLevel, Date nextVisitDate, String dsdCategory, Date tptStartDate,
	    Date tptCompletedDate, Date tptDiscontinuedDate, Date tuberculosisTreatmentEndDate,
	    Date dateViralLoadResultsReceived, String viralLoadTestStatus, Date VLEligibilityDate) {
		this.patientName = patientName;
		this.mrn = mrn;
		this.uan = uan;
		this.currentAge = currentAge;
		this.ageAtEnrollment = ageAtEnrollment;
		this.sex = sex;
		this.mobileNumber = mobileNumber;
		this.hivConfirmedDate = hivConfirmedDate;
		this.artStartDate = artStartDate;
		this.followupDate = followupDate;
		this.weightInKg = weightInKg;
		this.pregnancyStatus = pregnancyStatus;
		this.regimen = regimen;
		this.arvDoseDays = arvDoseDays;
		this.followUpStatus = followUpStatus;
		this.anitiretroviralAdherenceLevel = anitiretroviralAdherenceLevel;
		this.nextVisitDate = nextVisitDate;
		this.dsdCategory = dsdCategory;
		this.tptStartDate = tptStartDate;
		this.tptCompletedDate = tptCompletedDate;
		this.tptDiscontinuedDate = tptDiscontinuedDate;
		this.tuberculosisTreatmentEndDate = tuberculosisTreatmentEndDate;
		this.dateViralLoadResultsReceived = dateViralLoadResultsReceived;
		this.viralLoadTestStatus = viralLoadTestStatus;
		this.VLEligibilityDate = VLEligibilityDate;
	}
	
	public String getPatientName() {
		return patientName;
	}
	
	public String getMrn() {
		return mrn;
	}
	
	public String getUan() {
		return uan;
	}
	
	public int getCurrentAge() {
		return currentAge;
	}
	
	public int getAgeAtEnrollment() {
		return ageAtEnrollment;
	}
	
	public String getSex() {
		return sex;
	}
	
	public String getMobileNumber() {
		return mobileNumber;
	}
	
	public Date getHivConfirmedDate() {
		return hivConfirmedDate;
	}
	
	public Date getArtStartDate() {
		return artStartDate;
	}
	
	public Date getFollowupDate() {
		return followupDate;
	}
	
	public int getWeightInKg() {
		return weightInKg;
	}
	
	public String getPregnancyStatus() {
		return pregnancyStatus;
	}
	
	public String getRegimen() {
		return regimen;
	}
	
	public String getArvDoseDays() {
		return arvDoseDays;
	}
	
	public String getFollowUpStatus() {
		return followUpStatus;
	}
	
	public String getAnitiretroviralAdherenceLevel() {
		return anitiretroviralAdherenceLevel;
	}
	
	public Date getNextVisitDate() {
		return nextVisitDate;
	}
	
	public String getDsdCategory() {
		return dsdCategory;
	}
	
	public Date getTptStartDate() {
		return tptStartDate;
	}
	
	public Date getTptCompletedDate() {
		return tptCompletedDate;
	}
	
	public Date getTptDiscontinuedDate() {
		return tptDiscontinuedDate;
	}
	
	public Date getTuberculosisTreatmentEndDate() {
		return tuberculosisTreatmentEndDate;
	}
	
	public Date getDateViralLoadResultsReceived() {
		return dateViralLoadResultsReceived;
	}
	
	public String getViralLoadTestStatus() {
		return viralLoadTestStatus;
	}
	
	public Date getVLEligibilityDate() {
		return VLEligibilityDate;
	}
}
