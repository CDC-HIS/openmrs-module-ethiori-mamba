package org.openmrs.module.mambaetl.helpers.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DynamicDataMapper {
	
	public DynamicData mapResultSetToDynamicData(ResultSet rs) throws SQLException {
		DynamicData data = new DynamicData();
		int columnCount = rs.getMetaData().getColumnCount();
		
		for (int i = 1; i <= columnCount; i++) {
			String columnName = rs.getMetaData().getColumnName(i);
			Object value = rs.getObject(i);
			data.setField(columnName, value);
		}
		
		return data;
	}
}
