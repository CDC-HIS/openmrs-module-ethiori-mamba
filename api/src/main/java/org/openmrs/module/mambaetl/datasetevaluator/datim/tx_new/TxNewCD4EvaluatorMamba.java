package org.openmrs.module.mambaetl.datasetevaluator.datim.tx_new;

import org.openmrs.annotation.Handler;
import org.openmrs.module.mambacore.db.ConnectionPoolManager;
import org.openmrs.module.mambaetl.datasetdefinition.datim.TxNewCD4DataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.dto.CoarseData;
import org.openmrs.module.reporting.dataset.DataSet;
import org.openmrs.module.reporting.dataset.DataSetColumn;
import org.openmrs.module.reporting.dataset.DataSetRow;
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
import java.util.ArrayList;
import java.util.List;

import static org.hibernate.search.util.AnalyzerUtils.log;

@Handler(supports = { TxNewCD4DataSetDefinitionMamba.class })
public class TxNewCD4EvaluatorMamba implements DataSetEvaluator {
	
	@Override
	public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext evalContext) throws EvaluationException {
		TxNewCD4DataSetDefinitionMamba txNewCD4DataSetDefinitionMamba = (TxNewCD4DataSetDefinitionMamba) dataSetDefinition;
		SimpleDataSet data = new SimpleDataSet(dataSetDefinition, evalContext);
		DataSetRow row = new DataSetRow();
		// Check start date and end date are valid
		// If start date is greater than end date
		if (txNewCD4DataSetDefinitionMamba.getStartDate() != null && txNewCD4DataSetDefinitionMamba.getEndDate() != null
		        && txNewCD4DataSetDefinitionMamba.getStartDate().compareTo(txNewCD4DataSetDefinitionMamba.getEndDate()) > 0) {
			
			row.addColumnValue(new DataSetColumn("Error", "Error", Integer.class),
			    "Report start date cannot be after report end date");
			data.addRow(row);
			throw new EvaluationException("Start date can not be greater than end date");
		}
		//throw new EvaluationException("Start date cannot be greater than end date");
		List<CoarseData> resultSet = getEtlNew(txNewCD4DataSetDefinitionMamba);
		row.addColumnValue(new DataSetColumn("#", "#", Integer.class), "TOTAL");
		row.addColumnValue(new DataSetColumn("TX NEW CD4 EVALUATOR", "Patient Count", Integer.class), resultSet.size());
		data.addRow(row);
		for (CoarseData txNewDate : resultSet) {
			
			try {
				row = new DataSetRow();
				
				row.addColumnValue(new DataSetColumn("sex", "Sex", String.class), txNewDate.getSex());
				row.addColumnValue(new DataSetColumn("fifteen_plus", "15+", String.class), txNewDate.getAge_15_plus());
				row.addColumnValue(new DataSetColumn("fifteen_minus", "<15", String.class), txNewDate.getAge_15_minus());
				data.addRow(row);
				
			}
			catch (Exception e) {
				log.info("Exception mapping user dataset definition " + e.getMessage());
			}
			
		}
		return data;
		
	}
	
	private List<CoarseData> getEtlNew(TxNewCD4DataSetDefinitionMamba txNewCD4DataSetDefinitionMamba) {
        List<CoarseData> txCurrList = new ArrayList<>();
        DataSource dataSource = ConnectionPoolManager.getInstance().getDataSource();
        try (Connection connection = dataSource.getConnection();
             CallableStatement statement = connection.prepareCall("{call sp_dim_tx_new_datim_query(?,?,?)}")) {
            statement.setDate(1, new java.sql.Date(txNewCD4DataSetDefinitionMamba.getStartDate().getTime()));
            statement.setDate(2, new java.sql.Date(txNewCD4DataSetDefinitionMamba.getEndDate().getTime()));
			statement.setInt(3, 0);
			statement.setString(4, "all");
            boolean hasResults = statement.execute();

            while (hasResults) {
                try (ResultSet resultSet = statement.getResultSet()) {
                    while (resultSet.next()) { // Iterate through each row
						CoarseData data = mapRowToTxNewData(resultSet);
                        txCurrList.add(data);
                    }
                }
                hasResults = statement.getMoreResults(); // Check if there are more result sets
            }
        } catch (SQLException e) {
            log.info(e);
        }
        return txCurrList;
    }
	
	private CoarseData mapRowToTxNewData(ResultSet resultSet) throws SQLException {
		return new CoarseData(resultSet.getString("sex"), resultSet.getString("15+"), resultSet.getString("<15>"));
	}
}
