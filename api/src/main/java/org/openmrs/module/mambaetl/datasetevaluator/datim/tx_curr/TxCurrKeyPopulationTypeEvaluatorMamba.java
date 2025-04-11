package org.openmrs.module.mambaetl.datasetevaluator.datim.tx_curr;

import org.openmrs.annotation.Handler;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_curr.TxCurrAgeSexDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.ConnectionPoolManager;
import org.openmrs.module.mambaetl.helpers.mapper.ResultSetMapper;
import org.openmrs.module.reporting.dataset.DataSet;
import org.openmrs.module.reporting.dataset.SimpleDataSet;
import org.openmrs.module.reporting.dataset.definition.DataSetDefinition;
import org.openmrs.module.reporting.dataset.definition.evaluator.DataSetEvaluator;
import org.openmrs.module.reporting.evaluation.EvaluationContext;
import org.openmrs.module.reporting.evaluation.EvaluationException;

import javax.sql.DataSource;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

@Handler(supports = { TxCurrAgeSexDataSetDefinitionMamba.TxCurrKeyPopulationTypeDataSetDefinitionMamba.class })
public class TxCurrKeyPopulationTypeEvaluatorMamba implements DataSetEvaluator {
	
	@Override
    public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext evalContext) throws EvaluationException {
        TxCurrAgeSexDataSetDefinitionMamba.TxCurrKeyPopulationTypeDataSetDefinitionMamba dataSetDefinitionMamba =  (TxCurrAgeSexDataSetDefinitionMamba.TxCurrKeyPopulationTypeDataSetDefinitionMamba) dataSetDefinition;
        SimpleDataSet data = new SimpleDataSet(dataSetDefinition, evalContext);
        ResultSetMapper resultSetMapper = new ResultSetMapper();
        // Get ResultSet from the database
        try (Connection connection = getDataSource().getConnection();
             CallableStatement statement = connection.prepareCall("{call sp_dim_tx_new_datim_kp_query(?,?)}")) {
            statement.setDate(2, new java.sql.Date(dataSetDefinitionMamba.getEndDate().getTime()));


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
