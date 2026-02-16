package org.openmrs.module.mambaetl.api.db.impl;

import org.openmrs.module.mambaetl.api.db.PatientSummaryDao;
import org.openmrs.module.mambacore.db.ConnectionPoolManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class jdbcPatientSummaryDao implements PatientSummaryDao {
	
	private static final Logger log = LoggerFactory.getLogger(jdbcPatientSummaryDao.class);
	
	@Override
    public Map<String, Object> getPatientSummary(String patientUuid) {

        DataSource dataSource = ConnectionPoolManager
                .getInstance().getEtlDataSource();

        String sql = "SELECT " +
                "  * " +
                "FROM mamba_fact_client " +
                "WHERE patient_uuid = ? " +
                "LIMIT 1";

        Map<String, Object> result = null;

        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {

            // Bind the String UUID
            statement.setString(1, patientUuid);

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    result = new HashMap<>();
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int columnCount = metaData.getColumnCount();

                    for (int i = 1; i <= columnCount; i++) {
                        result.put(metaData.getColumnLabel(i), resultSet.getObject(i));
                    }
                }
            }

        } catch (SQLException e) {
            log.error("Failed to fetch patient summary for UUID: " + patientUuid, e);
            throw new RuntimeException("Database error in Patient Summary API", e);
        }

        return result;
    }
}
