package org.openmrs.module.mambaetl.datasetevaluator.datim.tx_new;

import org.openmrs.annotation.Handler;
import org.openmrs.module.mambacore.db.ConnectionPoolManager;
import org.openmrs.module.mambaetl.datasetdefinition.datim.TxNewFineAgeDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.mambaetl.helpers.dto.FineAgeData;
import org.openmrs.module.mambaetl.helpers.dto.TXNewData;
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
import java.util.Date;
import java.util.List;

import static org.hibernate.search.util.AnalyzerUtils.log;

@Handler(supports = { TxNewFineAgeDataSetDefinitionMamba.class })
public class TxNewFineAgeEvaluatorMamba implements DataSetEvaluator {
	
	@Override
	public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext evalContext) throws EvaluationException {
		TxNewFineAgeDataSetDefinitionMamba txNewFineAgeDataSetDefinitionMamba = (TxNewFineAgeDataSetDefinitionMamba) dataSetDefinition;
		SimpleDataSet data = new SimpleDataSet(dataSetDefinition, evalContext);
		DataSetRow row = new DataSetRow();
		// Check start date and end date are valid
		// If start date is greater than end date
		if (txNewFineAgeDataSetDefinitionMamba.getStartDate() != null
		        && txNewFineAgeDataSetDefinitionMamba.getEndDate() != null
		        && txNewFineAgeDataSetDefinitionMamba.getStartDate().compareTo(
		            txNewFineAgeDataSetDefinitionMamba.getEndDate()) > 0) {
			
			row.addColumnValue(new DataSetColumn("Error", "Error", Integer.class),
			    "Report start date cannot be after report end date");
			data.addRow(row);
			throw new EvaluationException("Start date can not be greater than end date");
		}
		//throw new EvaluationException("Start date cannot be greater than end date");
		List<FineAgeData> resultSet = getEtlNew(txNewFineAgeDataSetDefinitionMamba);
		row.addColumnValue(new DataSetColumn("#", "#", Integer.class), "TOTAL");
		row.addColumnValue(new DataSetColumn("Patient Count", "Patient Count", Integer.class), resultSet.size());
		data.addRow(row);
		for (FineAgeData fineAgeData : resultSet) {
			
			try {
				row = new DataSetRow();
				
				row.addColumnValue(new DataSetColumn("sex", "Sex", String.class), fineAgeData.getSex());
				row.addColumnValue(new DataSetColumn("1-4", "1-4", String.class), fineAgeData.getAge_1_4());
				row.addColumnValue(new DataSetColumn("5-9", "5-9", String.class), fineAgeData.getAge_5_9());
				row.addColumnValue(new DataSetColumn("10-14", "10-14", String.class), fineAgeData.getAge_10_14());
				row.addColumnValue(new DataSetColumn("15-19", "15-19", String.class), fineAgeData.getAge_15_19());
				row.addColumnValue(new DataSetColumn("20-24", "20-24", String.class), fineAgeData.getAge_20_24());
				row.addColumnValue(new DataSetColumn("25-29", "25-29", String.class), fineAgeData.getAge_25_29());
				row.addColumnValue(new DataSetColumn("30-34", "30-34", String.class), fineAgeData.getAge_30_34());
				row.addColumnValue(new DataSetColumn("35-39", "35-39", String.class), fineAgeData.getAge_35_39());
				row.addColumnValue(new DataSetColumn("40-44", "40-44", String.class), fineAgeData.getAge_40_44());
				row.addColumnValue(new DataSetColumn("45-49", "45-49", String.class), fineAgeData.getAge_45_49());
				row.addColumnValue(new DataSetColumn("50-54", "50-54", String.class), fineAgeData.getAge_50_54());
				row.addColumnValue(new DataSetColumn("55-59", "55-59", String.class), fineAgeData.getAge_55_59());
				row.addColumnValue(new DataSetColumn("60-64", "60-64", String.class), fineAgeData.getAge_60_64());
				row.addColumnValue(new DataSetColumn("65+", "65+", String.class), fineAgeData.getAge_65_plus());
				
				data.addRow(row);
				
			}
			catch (Exception e) {
				log.info("Exception mapping user dataset definition " + e.getMessage());
			}
			
		}
		return data;
		
	}
	
	private List<FineAgeData> getEtlNew(TxNewFineAgeDataSetDefinitionMamba txNewFineAgeDataSetDefinitionMamba) {
        List<FineAgeData> txCurrList = new ArrayList<>();
        DataSource dataSource = ConnectionPoolManager.getInstance().getDataSource();
        try (Connection connection = dataSource.getConnection();
             CallableStatement statement = connection.prepareCall("{call sp_dim_tx_new_datim_query(?,?,?,?)}")) {
            statement.setDate(1, new java.sql.Date(txNewFineAgeDataSetDefinitionMamba.getStartDate().getTime()));
            statement.setDate(2, new java.sql.Date(txNewFineAgeDataSetDefinitionMamba.getEndDate().getTime()));
            statement.setInt(3, 1);
            statement.setString(4, "All");
            boolean hasResults = statement.execute();

            while (hasResults) {
                try (ResultSet resultSet = statement.getResultSet()) {
                    while (resultSet.next()) { // Iterate through each row
                        FineAgeData data = mapRowToTxNewData(resultSet);
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
	
	private FineAgeData mapRowToTxNewData(ResultSet resultSet) throws SQLException {
		return new FineAgeData(resultSet.getString("sex"), resultSet.getString("1-4"), resultSet.getString("5-9"),
		        resultSet.getString("10-14"), resultSet.getString("15-19"), resultSet.getString("20-24"),
		        resultSet.getString("25-29"), resultSet.getString("30-34"), resultSet.getString("35-39"),
		        resultSet.getString("40-44"), resultSet.getString("45-49"), resultSet.getString("50-54"),
		        resultSet.getString("55-59"), resultSet.getString("60-64"), resultSet.getString("65+"));
	}
	
}
