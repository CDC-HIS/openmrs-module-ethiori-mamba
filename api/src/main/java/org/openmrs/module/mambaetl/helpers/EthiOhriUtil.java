package org.openmrs.module.mambaetl.helpers;

import org.openmrs.module.reporting.evaluation.parameter.Mapped;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.evaluation.parameter.Parameterizable;
import org.openmrs.module.reporting.evaluation.parameter.ParameterizableUtil;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

public class EthiOhriUtil {
	
	public static List<Parameter> getDateRangeParameters(Boolean required) {
		Parameter startDate = new Parameter("startDate", "Start Date", Date.class);
		startDate.setRequired(required);
		Parameter endDate = new Parameter("endDate", "End Date", Date.class);
		endDate.setRequired(required);
		return Arrays.asList(startDate, endDate);
	}
	
	public static DefaultDateParameter getDefaultDateParameter(java.util.Date startDate, java.util.Date endDate) {
		java.sql.Date _startDate = startDate != null ? new java.sql.Date(startDate.getTime()) : new java.sql.Date(LocalDate
		        .of(1900, 1, 1).toEpochDay() * 24 * 60 * 60 * 1000);
		
		java.sql.Date _endDate = endDate != null ? new java.sql.Date(endDate.getTime()) : new java.sql.Date(
		        System.currentTimeMillis());
		DefaultDateParameter result = new DefaultDateParameter(_startDate, _endDate);
		return result;
	}
	
	public static List<Parameter> getEndDateParameters(Boolean required) {
		Parameter endDate = new Parameter("endDate", "On Month", Date.class);
		endDate.setRequired(required);
		return Collections.singletonList(endDate);
	}
	
	public static <T extends Parameterizable> Mapped<T> map(T parameterizable, String mappings) {
		if (parameterizable == null) {
			throw new IllegalArgumentException("Parameterizable cannot be null");
		}
		if (mappings == null) {
			mappings = "";
		}
		return new Mapped<>(parameterizable, ParameterizableUtil.createParameterMappings(mappings));
	}
	
	public static EthiopianDate getEthiopiaDate(Date date) {
		if (date == null) {
			return null;
		}
		LocalDate lDate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
		EthiopianDate ethiopianDate = null;
		try {
			ethiopianDate = EthiopianDateConverter.ToEthiopianDate(lDate);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return ethiopianDate;
	}
	
	public static String getEthiopianDate(Date date) {
		if (date == null) {
			return "--";
		}
		LocalDate lDate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
		EthiopianDate ethiopianDate = null;
		try {
			ethiopianDate = EthiopianDateConverter.ToEthiopianDate(lDate);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return ethiopianDate == null ? "" : ethiopianDate.getDay() + "/" + ethiopianDate.getMonth() + "/"
		        + ethiopianDate.getYear();
	}
}
