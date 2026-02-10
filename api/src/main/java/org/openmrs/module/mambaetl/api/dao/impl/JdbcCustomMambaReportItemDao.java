package org.openmrs.module.mambaetl.api.dao.impl;

import org.openmrs.module.mambaetl.api.dao.CustomMambaReportItemDao;
import org.openmrs.module.mambaetl.api.model.CustomMambaReportItem;
import org.openmrs.module.mambaetl.api.model.CustomMambaReportItemColumn;
import org.openmrs.module.mambaetl.helpers.CustomConnectionPoolManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JdbcCustomMambaReportItemDao implements CustomMambaReportItemDao {

    private static final Logger log = LoggerFactory.getLogger(JdbcCustomMambaReportItemDao.class);

    @Override
    public List<CustomMambaReportItem> getClientByUuid(String patientUuid) {
        // Define your specific query here
        String sql = "SELECT * FROM mamba_dim_client WHERE patient_uuid = ?";
        return executeSqlQuery(sql, Collections.singletonList(patientUuid));
    }

    @Override
    public List<CustomMambaReportItem> executeSqlQuery(String sql, List<Object> params) {
        List<CustomMambaReportItem> mambaReportItems = new ArrayList<>();
        DataSource dataSource = CustomConnectionPoolManager.getInstance().getDataSource();

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            // Bind parameters
            if (params != null) {
                for (int i = 0; i < params.size(); i++) {
                    statement.setObject(i + 1, params.get(i));
                }
            }

            try (ResultSet resultSet = statement.executeQuery()) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                int serialId = 1;

                while (resultSet.next()) {
                    CustomMambaReportItem reportItem = new CustomMambaReportItem();
                    reportItem.setSerialId(serialId++);

                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        Object columnValue = resultSet.getObject(i);
                        reportItem.getRecord().add(new CustomMambaReportItemColumn(columnName, columnValue));
                    }
                    mambaReportItems.add(reportItem);
                }
            }
        } catch (SQLException e) {
            log.error("Failed to execute direct SQL query: " + sql, e);
        }
        return mambaReportItems;
    }
}