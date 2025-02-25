package org.openmrs.module.mambaetl.datasetevaluator.hmis_dhis2;

import org.openmrs.annotation.Handler;
import org.openmrs.module.mambaetl.datasetdefinition.hmis_dhis2.HMISDHIS2DatasetDefinition;
import org.openmrs.module.mambaetl.helpers.ConnectionPoolManager;
import org.openmrs.module.mambaetl.helpers.ValidationHelper;
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

@Handler(supports = {HMISDHIS2DatasetDefinition.class})
public class HMISDHIS2DataSetEvaluator implements DataSetEvaluator {

    @Override
    public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext evalContext)
            throws EvaluationException {

        HMISDHIS2DatasetDefinition hmisdhis2DatasetDefinition = (HMISDHIS2DatasetDefinition) dataSetDefinition;
        SimpleDataSet data = new SimpleDataSet(dataSetDefinition, evalContext);

        ValidationHelper validationHelper = new ValidationHelper();
        ResultSetMapper resultSetMapper = new ResultSetMapper();

        // Validate start and end dates
        validationHelper.validateDates(hmisdhis2DatasetDefinition, data);

        // Get ResultSet from the database
        try (Connection connection = getDataSource().getConnection()) {
            connection.setAutoCommit(false); // Ensure consistency across multiple queries

            try (CallableStatement statement1 = connection.prepareCall("{call sp_fact_dhis_tx_curr_query(?)}");
                 CallableStatement statement2 = connection.prepareCall("{call sp_fact_dhis_tx_new_query(?,?)}");
                 CallableStatement statement3 = connection.prepareCall("{call sp_fact_hmis_hiv_art_ret_net_query(?,?)}");
                 CallableStatement statement4 = connection.prepareCall("{call sp_fact_hmis_hiv_art_ret_query(?,?)}");

                 CallableStatement statement5 = connection.prepareCall("{call sp_fact_hmis_hiv_dsd_query(?)}");
                 CallableStatement statement6 = connection.prepareCall("{call sp_fact_hmis_hiv_pep_query(?)}");
                 CallableStatement statement7 = connection.prepareCall("{call sp_fact_hmis_hiv_prep_query(?,?)}");
                 CallableStatement statement8 = connection.prepareCall("{call sp_fact_hmis_hiv_tb_scrn_query(?,?)}");
                 CallableStatement statement9 = connection.prepareCall("{call sp_fact_hmis_hiv_tpt_query(?,?)}");
                 CallableStatement statement10 = connection.prepareCall("{call sp_fact_hmis_hiv_tx_pvls_query(?,?)}");

            ) {

                java.sql.Date startDate = new java.sql.Date(hmisdhis2DatasetDefinition.getStartDate().getTime());
                java.sql.Date endDate = new java.sql.Date(hmisdhis2DatasetDefinition.getEndDate().getTime());

                // 1. Execute tx curr
                statement1.setDate(1, startDate);
                statement1.setDate(2, endDate);
                ResultSet resultSet1 = statement1.executeQuery();

                // 2. Execute tx new
                statement2.setDate(1, startDate);
                statement2.setDate(2, endDate);
                ResultSet resultSet2 = statement2.executeQuery();

                // 3. Execute art net ret
                statement3.setDate(1, startDate);
                statement3.setDate(2, endDate);
                ResultSet resultSet3 = statement3.executeQuery();

                // 4. Execute art  ret
                statement4.setDate(1, startDate);
                statement4.setDate(2, endDate);
                ResultSet resultSet4 = statement4.executeQuery();

                // 5. Execute dsd
                statement5.setDate(1, endDate);
                ResultSet resultSet5 = statement5.executeQuery();

                // 6. Execute pep
                statement6.setDate(2, endDate);
                ResultSet resultSet6 = statement6.executeQuery();

                // 7. Execute prep
                statement7.setDate(1, startDate);
                statement7.setDate(2, endDate);
                ResultSet resultSet7 = statement7.executeQuery();

                // 8. Execute tb scrn
                statement8.setDate(1, startDate);
                statement8.setDate(2, endDate);
                ResultSet resultSet8 = statement8.executeQuery();

                // 9. Execute tpt
                statement9.setDate(1, startDate);
                statement9.setDate(2, endDate);
                ResultSet resultSet9 = statement9.executeQuery();

                // 10. Execute pvls
                statement10.setDate(1, startDate);
                statement10.setDate(2, endDate);
                ResultSet resultSet10 = statement10.executeQuery();

                ResultSet[] allResultSets=new ResultSet[]{resultSet1, resultSet2, resultSet3, resultSet4, resultSet5,
                resultSet6, resultSet7, resultSet8, resultSet9, resultSet10};
                // Merge results
                mapResultSet(data, resultSetMapper,allResultSets);
                connection.commit();
                return data;

            } catch (SQLException e) {
                connection.rollback();
                throw new EvaluationException("Error processing ResultSet: " + e.getMessage(), e);
            }
        } catch (SQLException e) {
            throw new EvaluationException("Database connection error: " + e.getMessage(), e);
        }
    }

    private void mapResultSet(SimpleDataSet data, ResultSetMapper resultSetMapper, ResultSet[] resultSet) throws SQLException {
        for (ResultSet set : resultSet) {
            if (set != null) {
                resultSetMapper.mapResultSetToDataSet(set, data);
            }
        }
    }

    private DataSource getDataSource() {
        return ConnectionPoolManager.getInstance().getDataSource();
    }
}
