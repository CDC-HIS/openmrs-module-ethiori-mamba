package org.openmrs.module.mambaetl.helpers;

import java.sql.Date;

public class DefaultDateParameter {
	
	public final Date startDate;
	
	public final Date endDate;
	
	public DefaultDateParameter(Date startDate, Date endDate) {
		this.startDate = startDate;
		this.endDate = endDate;
	}
}
