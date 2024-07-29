package org.openmrs.module.mambaetl.datasetevaluator.linelist.tx_new;

import org.openmrs.annotation.Handler;
import org.openmrs.module.mambacore.db.ConnectionPoolManager;
import org.openmrs.module.mambaetl.datasetdefinition.linelist.TXNewDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
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

@Handler(supports = { TXNewDataSetDefinitionMamba.class })
public class TxNewDatasetEvaluatorMamba implements DataSetEvaluator {
	
	@Override
	public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext evalContext) throws EvaluationException {
		TXNewDataSetDefinitionMamba txNewDataSetDefinitionMamba = (TXNewDataSetDefinitionMamba) dataSetDefinition;
		SimpleDataSet data = new SimpleDataSet(dataSetDefinition, evalContext);
		DataSetRow row = new DataSetRow();
		// Check start date and end date are valid
		// If start date is greater than end date
		if (txNewDataSetDefinitionMamba.getStartDate() != null && txNewDataSetDefinitionMamba.getEndDate() != null
		        && txNewDataSetDefinitionMamba.getStartDate().compareTo(txNewDataSetDefinitionMamba.getEndDate()) > 0) {
			
			row.addColumnValue(new DataSetColumn("Error", "Error", Integer.class),
			    "Report start date cannot be after report end date");
			data.addRow(row);
			throw new EvaluationException("Start date can not be greater than end date");
		}
		//throw new EvaluationException("Start date cannot be greater than end date");
		List<TXNewData> resultSet = getEtlNew(txNewDataSetDefinitionMamba);
		row.addColumnValue(new DataSetColumn("#", "#", Integer.class), "TOTAL");
		row.addColumnValue(new DataSetColumn("Patient Count", "Patient Count", Integer.class), resultSet.size());
		data.addRow(row);
		for (TXNewData txNewDate : resultSet) {
			
			try {
				row = new DataSetRow();
				
				row.addColumnValue(new DataSetColumn("patientName", "Patient name", String.class),
				    txNewDate.getPatientName());
				row.addColumnValue(new DataSetColumn("mrn", "MRN", String.class), txNewDate.getMrn());
				row.addColumnValue(new DataSetColumn("uan", "UAN", String.class), txNewDate.getUan());
				row.addColumnValue(new DataSetColumn("currentAge", "Current Age", Integer.class), txNewDate.getCurrentAge());
				row.addColumnValue(new DataSetColumn("sex", "Sex", String.class), txNewDate.getSex());
				row.addColumnValue(new DataSetColumn("mobileNumber", "Mobile Number", String.class),
				    txNewDate.getMobileNumber());
				row.addColumnValue(new DataSetColumn("weightInKg", "Weight In KG", Integer.class), txNewDate.getWeightInKg());
				row.addColumnValue(new DataSetColumn("cd4Count", "CD4 Count", Integer.class), txNewDate.getCd4Count());
				row.addColumnValue(new DataSetColumn("currentWhoHivStage", "Current WHO HIV Stage", String.class),
				    txNewDate.getCurrentWhoHivStage());
				row.addColumnValue(new DataSetColumn("nutritionalStatus", "Nutritional Status", String.class),
				    txNewDate.getNutritionalStatus());
				row.addColumnValue(new DataSetColumn("tbScreeningResult", "TB Screening Result", String.class),
				    txNewDate.getTbScreeningResult());
				row.addColumnValue(
				    new DataSetColumn("enrollmentDate", "Enrollment Date", Date.class),
				    txNewDate.getEnrollmentDate() != null ? EthiOhriUtil.getEthiopianDate(new java.util.Date(txNewDate
				            .getEnrollmentDate().getTime())) : txNewDate.getEnrollmentDate());
				row.addColumnValue(
				    new DataSetColumn("hivConfirmedDate", "HIV Confirmed Date", Date.class),
				    txNewDate.getHivConfirmedDate() != null ? EthiOhriUtil.getEthiopianDate(new java.util.Date(txNewDate
				            .getHivConfirmedDate().getTime())) : txNewDate.getHivConfirmedDate());
				row.addColumnValue(
				    new DataSetColumn("artStartDate", "ART Start Date", Date.class),
				    txNewDate.getArtStartDate() != null ? EthiOhriUtil.getEthiopianDate(new java.util.Date(txNewDate
				            .getArtStartDate().getTime())) : txNewDate.getArtStartDate());
				row.addColumnValue(new DataSetColumn("daysDifference", "Days difference", Integer.class),
				    txNewDate.getDaysDifference());
				row.addColumnValue(
				    new DataSetColumn("followupDate", "Followup Date", Date.class),
				    txNewDate.getFollowupDate() != null ? EthiOhriUtil.getEthiopianDate(new java.util.Date(txNewDate
				            .getFollowupDate().getTime())) : txNewDate.getFollowupDate());
				row.addColumnValue(new DataSetColumn("regimen", "Regimen", String.class), txNewDate.getRegimen());
				row.addColumnValue(new DataSetColumn("arvDoseDays", "ARV Dose Days", String.class),
				    txNewDate.getArvDoseDays());
				row.addColumnValue(new DataSetColumn("pregnancyStatus", "Pregnancy Status", String.class),
				    txNewDate.getPregnancyStatus());
				row.addColumnValue(new DataSetColumn("breastFeedingStatus", "Breast Feeding Status", String.class),
				    txNewDate.getBreastFeedingStatus());
				row.addColumnValue(new DataSetColumn("followUpStatus", "Followup Status", String.class),
				    txNewDate.getFollowUpStatus());
				row.addColumnValue(new DataSetColumn("ti", "TI", String.class), txNewDate.getTi());
				row.addColumnValue(
				    new DataSetColumn("treatmentEndDate", "Treatment End Date", Date.class),
				    txNewDate.getTreatmentEndDate() != null ? EthiOhriUtil.getEthiopianDate(new java.util.Date(txNewDate
				            .getTreatmentEndDate().getTime())) : txNewDate.getTreatmentEndDate());
				row.addColumnValue(
				    new DataSetColumn("nextVisitDate", "Next Visit Date", Date.class),
				    txNewDate.getNextVisitDate() != null ? EthiOhriUtil.getEthiopianDate(new java.util.Date(txNewDate
				            .getNextVisitDate().getTime())) : txNewDate.getNextVisitDate());
				row.addColumnValue(
				    new DataSetColumn("latestFollowupDate", "Latest Followup Date", Date.class),
				    txNewDate.getLatestFollowupDate() != null ? EthiOhriUtil.getEthiopianDate(new java.util.Date(txNewDate
				            .getLatestFollowupDate().getTime())) : txNewDate.getLatestFollowupDate());
				row.addColumnValue(new DataSetColumn("latestFollowupStatus", "Latest Followup Status", String.class),
				    txNewDate.getLatestFollowupStatus());
				row.addColumnValue(new DataSetColumn("latestRegimen", "Latest Regimen", String.class),
				    txNewDate.getLatestRegimen());
				row.addColumnValue(new DataSetColumn("latestArvDoseDays", "Latest ARV Dose Days", String.class),
				    txNewDate.getLatestArvDoseDays());
				
				data.addRow(row);
				
			}
			catch (Exception e) {
				log.info("Exception mapping user dataset definition " + e.getMessage());
			}
			
		}
		return data;
		
	}
	
	private List<TXNewData> getEtlNew(TXNewDataSetDefinitionMamba txNewDataSetDefinitionMamba) {
        List<TXNewData> txCurrList = new ArrayList<>();
        DataSource dataSource = ConnectionPoolManager.getInstance().getDataSource();
        try (Connection connection = dataSource.getConnection();
             CallableStatement statement = connection.prepareCall("{call sp_fact_encounter_art_follow_up_tx_new_query(?,?)}")) {
            statement.setDate(1, new java.sql.Date(txNewDataSetDefinitionMamba.getStartDate().getTime()));
            statement.setDate(2, new java.sql.Date(txNewDataSetDefinitionMamba.getEndDate().getTime()));
            boolean hasResults = statement.execute();

            while (hasResults) {
                try (ResultSet resultSet = statement.getResultSet()) {
                    while (resultSet.next()) { // Iterate through each row
                        TXNewData data = mapRowToTxNewData(resultSet);
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
	
	private TXNewData mapRowToTxNewData(ResultSet resultSet) throws SQLException {
		return new TXNewData(resultSet.getString("patient_name"), resultSet.getString("mrn"), resultSet.getString("uan"),
		        resultSet.getInt("current_age"), resultSet.getString("sex"), resultSet.getString("mobile_no"),
		        resultSet.getInt("weight_in_kg"), resultSet.getInt("cd4_count"),
		        resultSet.getString("current_who_hiv_stage"), resultSet.getString("nutritional_status"),
		        resultSet.getString("tb_screening_result"), resultSet.getDate("enrollment_date"),
		        resultSet.getDate("hiv_confirmed_date"), resultSet.getDate("art_start_date"),
		        resultSet.getInt("days_difference"), resultSet.getDate("followup_date"), resultSet.getString("regimen"),
		        resultSet.getString("arv_dose_days"), resultSet.getString("pregnancy_status"),
		        resultSet.getString("breast_feeding_status"), resultSet.getString("follow_up_status"),
		        resultSet.getString("ti"), resultSet.getDate("treatment_end_date"), resultSet.getDate("next_visit_date"),
		        resultSet.getDate("latest_followup_date"), resultSet.getString("latest_followup_status"),
		        resultSet.getString("latest_regimen"), resultSet.getString("latest_arv_dose_days")
		
		);
	}
}
