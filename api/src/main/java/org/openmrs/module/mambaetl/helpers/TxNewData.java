package org.openmrs.module.mambaetl.helpers;

import java.sql.Date;

public class TxNewData {
	
	private String patientName;
	
	private String mrn;
	
	private String uan;
	
	private int ageAtEnrollment;
	
	private int currentAge;
	
	private String sex;
	
	private String mobileNumber;
	
	private Date enrollmentDate;
	
	private Date hivConfirmedDate;
	
	private Date artStartDate;
	
	private int daysDifference;
	
	private String pregnancyStatus;
	
	private String regimenAtEnrollment;
	
	private String arvDoseAtEnrollment;
	
	private String lastFollowUpStatus;
	
	private Date nextVisitDate;
	
	public TxNewData(String patientName, String mrn, String uan, int ageAtEnrollment, int currentAge, String sex,
	    String mobileNumber, Date enrollmentDate, Date hivConfirmedDate, Date artStartDate, int daysDifference,
	    String pregnancyStatus, String regimenAtEnrollment, String arvDoseAtEnrollment, String lastFollowUpStatus,
	    Date nextVisitDate) {
		this.patientName = patientName;
		this.mrn = mrn;
		this.uan = uan;
		this.ageAtEnrollment = ageAtEnrollment;
		this.currentAge = currentAge;
		this.sex = sex;
		this.mobileNumber = mobileNumber;
		this.enrollmentDate = enrollmentDate;
		this.hivConfirmedDate = hivConfirmedDate;
		this.artStartDate = artStartDate;
		this.daysDifference = daysDifference;
		this.pregnancyStatus = pregnancyStatus;
		this.regimenAtEnrollment = regimenAtEnrollment;
		this.arvDoseAtEnrollment = arvDoseAtEnrollment;
		this.lastFollowUpStatus = lastFollowUpStatus;
		this.nextVisitDate = nextVisitDate;
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
	
	public int getAgeAtEnrollment() {
		return ageAtEnrollment;
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
	
	public String getPregnancyStatus() {
		return pregnancyStatus;
	}
	
	public String getRegimenAtEnrollment() {
		return regimenAtEnrollment;
	}
	
	public String getArvDoseAtEnrollment() {
		return arvDoseAtEnrollment;
	}
	
	public String getLastFollowUpStatus() {
		return lastFollowUpStatus;
	}
	
	public Date getNextVisitDate() {
		return nextVisitDate;
	}
}