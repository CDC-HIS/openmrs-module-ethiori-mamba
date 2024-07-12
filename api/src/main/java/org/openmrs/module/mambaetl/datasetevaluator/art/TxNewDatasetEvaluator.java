package org.openmrs.module.mambaetl.datasetevaluator.art;

import org.openmrs.annotation.Handler;
import org.openmrs.module.mambacore.db.ConnectionPoolManager;
import org.openmrs.module.mambaetl.datasetdefinition.linelist.HTSNewDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.TxNewData;
import org.openmrs.module.reporting.dataset.DataSet;
import org.openmrs.module.reporting.dataset.DataSetColumn;
import org.openmrs.module.reporting.dataset.DataSetRow;
import org.openmrs.module.reporting.dataset.SimpleDataSet;
import org.openmrs.module.reporting.dataset.definition.DataSetDefinition;
import org.openmrs.module.reporting.dataset.definition.evaluator.DataSetEvaluator;
import org.openmrs.module.reporting.evaluation.EvaluationContext;
import org.openmrs.module.reporting.evaluation.EvaluationException;

import javax.sql.DataSource;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

@Handler(supports = { HTSNewDataSetDefinitionMamba.class })
public class TxNewDatasetEvaluator implements DataSetEvaluator {
	
	@Override
	public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext evalContext) throws EvaluationException {
		HTSNewDataSetDefinitionMamba htsNewDataSetDefinitionMamba = (HTSNewDataSetDefinitionMamba) dataSetDefinition;
		SimpleDataSet data = new SimpleDataSet(dataSetDefinition, evalContext);
		
		// Check start date and end date are valid
		// If start date is greater than end date
		if (htsNewDataSetDefinitionMamba.getStartDate() != null && htsNewDataSetDefinitionMamba.getEndDate() != null
		        && htsNewDataSetDefinitionMamba.getStartDate().compareTo(htsNewDataSetDefinitionMamba.getEndDate()) > 0) {
			throw new EvaluationException("Start date can not be greater than end date");
		}
		//throw new EvaluationException("Start date cannot be greater than end date");
		DataSetRow row = new DataSetRow();
		row.addColumnValue(new DataSetColumn("Error", "Error", Integer.class),
		    "Report start date cannot be after report end date");
		data.addRow(row);
		List<TxNewData> resultSet = getEtlNew(htsNewDataSetDefinitionMamba);
		
		for (TxNewData txNewDate : resultSet) {
			
			try {
				row = new DataSetRow();
				
				row.addColumnValue(new DataSetColumn("patientName", "Patient name", String.class),
				    txNewDate.getPatientName());
				row.addColumnValue(new DataSetColumn("mrn", "MRN", String.class), txNewDate.getMrn());
				row.addColumnValue(new DataSetColumn("uan", "UAN", String.class), txNewDate.getUan());
				row.addColumnValue(new DataSetColumn("ageAtEnrollment", "Age At Enrollment", Integer.class),
				    txNewDate.getAgeAtEnrollment());
				row.addColumnValue(new DataSetColumn("currentAge", "Current Age", Integer.class), txNewDate.getCurrentAge());
				row.addColumnValue(new DataSetColumn("sex", "Sex", String.class), txNewDate.getSex());
				row.addColumnValue(new DataSetColumn("mobileNumber", "Mobile Number", String.class),
				    txNewDate.getMobileNumber());
				row.addColumnValue(new DataSetColumn("enrollmentDate", "Enrollment Date", Date.class),
				    txNewDate.getEnrollmentDate());
				row.addColumnValue(new DataSetColumn("hivConfirmedDate", "HIV Confirmed Date", Date.class),
				    txNewDate.getHivConfirmedDate());
				row.addColumnValue(new DataSetColumn("artStartDate", "ART Start Date", Date.class),
				    txNewDate.getArtStartDate());
				row.addColumnValue(new DataSetColumn("daysDifference", "Days Difference", Integer.class),
				    txNewDate.getDaysDifference());
				row.addColumnValue(new DataSetColumn("pregnancyStatus", "Pregnancy Status", String.class),
				    txNewDate.getPregnancyStatus());
				row.addColumnValue(new DataSetColumn("regimenAtEnrollment", "Regiment At Enrollment", String.class),
				    txNewDate.getRegimenAtEnrollment());
				row.addColumnValue(new DataSetColumn("arvDoseAtEnrollment", "ARV dose at enrollment", String.class),
				    txNewDate.getArvDoseAtEnrollment());
				row.addColumnValue(new DataSetColumn("lastFollowUpStatus", "Last followup status", String.class),
				    txNewDate.getLastFollowUpStatus());
				row.addColumnValue(new DataSetColumn("nextVisitDate", "Next Visit Date", Date.class),
				    txNewDate.getNextVisitDate());
				
				data.addRow(row);
				
			}
			catch (Exception e) {
				e.printStackTrace();
			}
			
		}
		return data;
		
	}
	
	private List<TxNewData> getEtlNew(HTSNewDataSetDefinitionMamba htsNewDataSetDefinitionMamba) {
        List<TxNewData> txCurrList = new ArrayList<>();
        DataSource dataSource = ConnectionPoolManager.getInstance().getDataSource();

        try (Connection connection = dataSource.getConnection();
             CallableStatement statement = connection.prepareCall("{call sp_fact_encounter_care_and_treatment_tx_new_query(?,?)}")) {
            statement.setDate(1, htsNewDataSetDefinitionMamba.getStartDate());
            statement.setDate(2, htsNewDataSetDefinitionMamba.getEndDate());
            boolean hasResults = statement.execute();

            while (hasResults) {
                try (ResultSet resultSet = statement.getResultSet()) {
                    while (resultSet.next()) { // Iterate through each row
                        TxNewData data = mapRowToTxNewData(resultSet);
                        txCurrList.add(data);
                    }
                }
                hasResults = statement.getMoreResults(); // Check if there are more result sets
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return txCurrList;
    }
	
	private TxNewData mapRowToTxNewData(ResultSet resultSet) throws SQLException {
		return new TxNewData(resultSet.getString("patient_name"), resultSet.getString("mrn"), resultSet.getString("uan"),
		        resultSet.getInt("age_at_enrollment"), resultSet.getInt("current_age"), resultSet.getString("sex"),
		        resultSet.getString("mobile_no"), resultSet.getDate("enrollment_date"),
		        resultSet.getDate("hiv_confirmed_date"), resultSet.getDate("art_start_date"),
		        resultSet.getInt("days_difference"), resultSet.getString("pregnancy_status"),
		        resultSet.getString("regimen_at_enrollment"), resultSet.getString("arv_dose_at_enrollment"),
		        resultSet.getString("last_follow_up_status"), resultSet.getDate("last_follow_up_date"));
	}
}
