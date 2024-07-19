package org.openmrs.module.mambaetl.helpers;

import java.util.Date;

public class TxNewData {
	
	private final String patientName;
	
	private final String mrn;
	
	private final String uan;
	
	private final int currentAge;
	
	private final String sex;
	
	private final String mobileNumber;
	
	private final int weightInKg;
	
	private final int cd4Count;
	
	private final String currentWhoHivStage;
	
	private final String nutritionalStatus;
	
	private final String tbScreeningResult;
	
	private final Date enrollmentDate;
	
	private final Date hivConfirmedDate;
	
	private final Date artStartDate;
	
	private final int daysDifference;
	
	private final Date followupDate;
	
	private final String regimen;
	
	private final String arvDoseDays;
	
	private final String pregnancyStatus;
	
	private final String breastFeedingStatus;
	
	private final String followUpStatus;
	
	private final String ti;
	
	private final Date treatmentEndDate;
	
	private final Date nextVisitDate;
	
	private final Date latestFollowupDate;
	
	private final String latestFollowupStatus;
	
	private final String latestRegimen;
	
	private final String latestArvDoseDays;
	
	public TxNewData(String patientName, String mrn, String uan, int currentAge, String sex, String mobileNumber,
	    int weightInKg, int cd4Count, String currentWhoHivStage, String nutritionalStatus, String tbScreeningResult,
	    Date enrollmentDate, Date hivConfirmedDate, Date artStartDate, int daysDifference, Date followupDate,
	    String regimen, String arvDoseDays, String pregnancyStatus, String breastFeedingStatus, String followUpStatus,
	    String ti, Date treatmentEndDate, Date nextVisitDate, Date latestFollowupDate, String latestFollowupStatus,
	    String latestRegimen, String latestArvDoseDays) {
		this.patientName = patientName;
		this.mrn = mrn;
		this.uan = uan;
		this.currentAge = currentAge;
		this.sex = sex;
		this.mobileNumber = mobileNumber;
		this.weightInKg = weightInKg;
		this.cd4Count = cd4Count;
		this.currentWhoHivStage = currentWhoHivStage;
		this.nutritionalStatus = nutritionalStatus;
		this.tbScreeningResult = tbScreeningResult;
		this.enrollmentDate = enrollmentDate;
		this.hivConfirmedDate = hivConfirmedDate;
		this.artStartDate = artStartDate;
		this.daysDifference = daysDifference;
		this.followupDate = followupDate;
		this.regimen = regimen;
		this.arvDoseDays = arvDoseDays;
		this.pregnancyStatus = pregnancyStatus;
		this.breastFeedingStatus = breastFeedingStatus;
		this.followUpStatus = followUpStatus;
		this.ti = ti;
		this.treatmentEndDate = treatmentEndDate;
		this.nextVisitDate = nextVisitDate;
		this.latestFollowupDate = latestFollowupDate;
		this.latestFollowupStatus = latestFollowupStatus;
		this.latestRegimen = latestRegimen;
		this.latestArvDoseDays = latestArvDoseDays;
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
	
	public String getSex() {
		return sex;
	}
	
	public String getMobileNumber() {
		return mobileNumber;
	}
	
	public int getWeightInKg() {
		return weightInKg;
	}
	
	public int getCd4Count() {
		return cd4Count;
	}
	
	public String getCurrentWhoHivStage() {
		return currentWhoHivStage;
	}
	
	public String getNutritionalStatus() {
		return nutritionalStatus;
	}
	
	public String getTbScreeningResult() {
		return tbScreeningResult;
	}
	
	public Date getEnrollmentDate() {
		return enrollmentDate;
	}
	
	public Date getHivConfirmedDate() {
		return hivConfirmedDate;
	}
	
	public Date getArtStartDate() {
		return artStartDate;
	}
	
	public int getDaysDifference() {
		return daysDifference;
	}
	
	public Date getFollowupDate() {
		return followupDate;
	}
	
	public String getRegimen() {
		return regimen;
	}
	
	public String getArvDoseDays() {
		return arvDoseDays;
	}
	
	public String getPregnancyStatus() {
		return pregnancyStatus;
	}
	
	public String getBreastFeedingStatus() {
		return breastFeedingStatus;
	}
	
	public String getFollowUpStatus() {
		return followUpStatus;
	}
	
	public String getTi() {
		return ti;
	}
	
	public Date getTreatmentEndDate() {
		return treatmentEndDate;
	}
	
	public Date getNextVisitDate() {
		return nextVisitDate;
	}
	
	public Date getLatestFollowupDate() {
		return latestFollowupDate;
	}
	
	public String getLatestFollowupStatus() {
		return latestFollowupStatus;
	}
	
	public String getLatestRegimen() {
		return latestRegimen;
	}
	
	public String getLatestArvDoseDays() {
		return latestArvDoseDays;
	}
	
}
