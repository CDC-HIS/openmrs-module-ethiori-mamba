package org.openmrs.module.mambaetl.datasetevaluator.linelist.dataExtractionTool;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.openmrs.annotation.Handler;
import org.openmrs.module.mambaetl.datasetdefinition.linelist.dataExtractionTool.CXCADataExtractionDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.ConnectionPoolManager;
import org.openmrs.module.mambaetl.helpers.ValidationHelper;
import org.openmrs.module.mambaetl.helpers.mapper.ResultSetMapper;
import org.openmrs.module.reporting.dataset.DataSet;
import org.openmrs.module.reporting.dataset.SimpleDataSet;
import org.openmrs.module.reporting.dataset.definition.DataSetDefinition;
import org.openmrs.module.reporting.dataset.definition.evaluator.DataSetEvaluator;
import org.openmrs.module.reporting.evaluation.EvaluationContext;
import org.openmrs.module.reporting.evaluation.EvaluationException;

@Handler(supports = { CXCADataExtractionDataSetDefinitionMamba.class })
public class CxCaDataExtractionDataSetDefinitionEvaluatorMamba implements DataSetEvaluator {
	
	@Override
    public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext evalContext)
            throws EvaluationException {
        CXCADataExtractionDataSetDefinitionMamba cxcaDataExtractionDataSetDefinitionMamba = (CXCADataExtractionDataSetDefinitionMamba) dataSetDefinition;

        SimpleDataSet data = new SimpleDataSet(dataSetDefinition, evalContext);

        ValidationHelper validationHelper = new ValidationHelper();
        ResultSetMapper resultSetMapper = new ResultSetMapper();

        // Validate start and end dates
        validationHelper.validateDates(cxcaDataExtractionDataSetDefinitionMamba, data);
        // Get ResultSet from the database
        try (Connection connection = getDataSource().getConnection();
                CallableStatement statement = connection
                        .prepareCall("{call sp_fact_cxca_linelist_data_extraction_query(?,?)}")) {
            statement.setDate(1, new java.sql.Date(cxcaDataExtractionDataSetDefinitionMamba.getStartDate().getTime()));
            statement.setDate(2, new java.sql.Date(cxcaDataExtractionDataSetDefinitionMamba.getEndDate().getTime()));

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet != null) {
                    return resultSetMapper.mapResultSetToDataSet(resultSet, data);
                } else {
                    throw new EvaluationException("No data returned from the query.");
                }
            }
        } catch (SQLException e) {
            throw new EvaluationException("Error processing ResultSet: " + e.getMessage(), e);
        }
    }
	
	private DataSource getDataSource() {
		return ConnectionPoolManager.getInstance().getDataSource();
	}
}
