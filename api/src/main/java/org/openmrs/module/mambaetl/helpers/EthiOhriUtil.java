package org.openmrs.module.mambaetl.helpers;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.openmrs.module.mambaetl.helpers.EthiopianDate;
import org.openmrs.module.mambaetl.helpers.EthiopianDateConverter;
import org.openmrs.module.reporting.evaluation.parameter.Mapped;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.evaluation.parameter.Parameterizable;
import org.openmrs.module.reporting.evaluation.parameter.ParameterizableUtil;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

public class EthiOhriUtil {
	
	private static final int MONTHS_IN_A_YEAR = 12;
	
	public static List<Parameter> getDateRangeParameters() {
		Parameter startDate = new Parameter("startDate", "Start Date", Date.class);
		startDate.setRequired(true);
		//		Parameter startDateGC = new Parameter("startDateGC", " ", Date.class);
		//		startDateGC.setRequired(false);
		Parameter endDate = new Parameter("endDate", "End Date", Date.class);
		endDate.setRequired(true);
		//		Parameter endDateGC = new Parameter("endDateGC", " ", Date.class);
		//		endDateGC.setRequired(false);
		return Arrays.asList(startDate, endDate);
	}
	
	@NotNull
	@Contract("null -> fail; !null -> new")
	public static <T extends Parameterizable> Mapped<T> map(T parameterizable) {
		if (parameterizable == null) {
			throw new IllegalArgumentException("Parameterizable cannot be null");
		}
		
		String mappings = "startDate=${startDate},endDate=${endDate}";
		return new Mapped<T>(parameterizable, ParameterizableUtil.createParameterMappings(mappings));
	}
	
	public static <T extends Parameterizable> Mapped<T> mapEndDate(T parameterizable) {
		if (parameterizable == null) {
			throw new IllegalArgumentException("Parameterizable cannot be null");
		}
		
		String mappings = "endDate=${endDate}";
		return new Mapped<T>(parameterizable, ParameterizableUtil.createParameterMappings(mappings));
	}
	
	public static <T extends Parameterizable> Mapped<T> mapNoDate(T parameterizable) {
		if (parameterizable == null) {
			throw new IllegalArgumentException("Parameterizable cannot be null");
		}
		
		String mappings = "endDate=${endDate}";
		return new Mapped<T>(parameterizable, ParameterizableUtil.createParameterMappings(mappings));
	}
	
	public static <T extends Parameterizable> Mapped<T> map(T parameterizable, String _map) {
		if (parameterizable == null) {
			throw new IllegalArgumentException("Parameterizable cannot be null");
		}
		
		String mappings = "startDate=${startDate},endDate=${endDate}";
		
		if (!Objects.isNull(_map) && !_map.isEmpty())
			mappings = _map + "," + mappings;
		
		return new Mapped<T>(parameterizable, ParameterizableUtil.createParameterMappings(mappings));
	}
	
	public static int getAgeInMonth(Date birthDate, Date asOfDate) {
		Calendar birthCalendar = Calendar.getInstance();
		Calendar asOfCalendar = Calendar.getInstance();
		birthCalendar.setTime(birthDate);
		asOfCalendar.setTime(asOfDate);
		
		int ageInMonthOfYear = (asOfCalendar.get(Calendar.YEAR) - birthCalendar.get(Calendar.YEAR)) * MONTHS_IN_A_YEAR;
		
		return (asOfCalendar.get(Calendar.MONTH) - birthCalendar.get(Calendar.MONTH)) + ageInMonthOfYear;
		
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ethiopianDate == null ? "" : ethiopianDate.getDay() + "/" + ethiopianDate.getMonth() + "/"
		        + ethiopianDate.getYear();
	}
	
}
