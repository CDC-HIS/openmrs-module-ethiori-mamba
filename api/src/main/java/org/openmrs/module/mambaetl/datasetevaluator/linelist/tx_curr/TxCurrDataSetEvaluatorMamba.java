package org.openmrs.module.mambaetl.datasetevaluator.linelist.tx_curr;

import org.openmrs.annotation.Handler;
import org.openmrs.module.mambacore.db.ConnectionPoolManager;
import org.openmrs.module.mambaetl.datasetdefinition.linelist.TxCurrDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.mambaetl.helpers.TxCurrData;
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
import java.util.*;

import static org.hibernate.search.util.AnalyzerUtils.log;

@Handler(supports = { TxCurrDataSetDefinitionMamba.class })
public class TxCurrDataSetEvaluatorMamba implements DataSetEvaluator {
	
	@Override
	public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext evalContext) throws EvaluationException {
		
		TxCurrDataSetDefinitionMamba txCurrDataSetDefinitionMamba = (TxCurrDataSetDefinitionMamba) dataSetDefinition;
		SimpleDataSet data = new SimpleDataSet(dataSetDefinition, evalContext);
		
		DataSetRow row = new DataSetRow();
		if (txCurrDataSetDefinitionMamba.getStartDate() != null && txCurrDataSetDefinitionMamba.getEndDate() != null
		        && txCurrDataSetDefinitionMamba.getStartDate().compareTo(txCurrDataSetDefinitionMamba.getEndDate()) > 0) {
			
			row.addColumnValue(new DataSetColumn("Error", "Error", Integer.class),
			    "Report start date cannot be after report end date");
			data.addRow(row);
			throw new EvaluationException("Start date can not be greater than end date");
		}
		//throw new EvaluationException("Start date cannot be greater than end date");
		List<TxCurrData> resultSet = getEtlCurr(txCurrDataSetDefinitionMamba);
		
		row.addColumnValue(new DataSetColumn("#", "#", Integer.class), "TOTAL");
		row.addColumnValue(new DataSetColumn("Patient Count", "Patient Count", Integer.class), resultSet.size());
		data.addRow(row);
		
		for (TxCurrData txCurrData : resultSet) {
			
			try {
				row = new DataSetRow();
				
				row.addColumnValue(new DataSetColumn("patientName", "Patient name", String.class),
				    txCurrData.getPatientName());
				row.addColumnValue(new DataSetColumn("mrn", "MRN", String.class), txCurrData.getMrn());
				row.addColumnValue(new DataSetColumn("uan", "UAN", String.class), txCurrData.getUan());
				row.addColumnValue(new DataSetColumn("currentAge", "Current Age", Integer.class), txCurrData.getCurrentAge());
				row.addColumnValue(new DataSetColumn("ageAtEnrollment", "Age At Enrollment", Integer.class),
				    txCurrData.getAgeAtEnrollment());
				row.addColumnValue(new DataSetColumn("sex", "Sex", String.class), txCurrData.getSex());
				row.addColumnValue(new DataSetColumn("mobileNumber", "Mobile Number", String.class),
				    txCurrData.getMobileNumber());
				row.addColumnValue(new DataSetColumn("weightInKg", "Weight In KG", Integer.class),
				    txCurrData.getWeightInKg());
				row.addColumnValue(
				    new DataSetColumn("hivConfirmedDate", "HIV Confirmed Date", Date.class),
				    txCurrData.getHivConfirmedDate() != null ? EthiOhriUtil.getEthiopianDate(new java.util.Date(txCurrData
				            .getHivConfirmedDate().getTime())) : txCurrData.getHivConfirmedDate());
				row.addColumnValue(
				    new DataSetColumn("artStartDate", "Art Start Date", Date.class),
				    txCurrData.getArtStartDate() != null ? EthiOhriUtil.getEthiopianDate(new java.util.Date(txCurrData
				            .getArtStartDate().getTime())) : txCurrData.getArtStartDate());
				row.addColumnValue(
				    new DataSetColumn("followupDate", "Follow Up Date", Date.class),
				    txCurrData.getFollowupDate() != null ? EthiOhriUtil.getEthiopianDate(new java.util.Date(txCurrData
				            .getFollowupDate().getTime())) : txCurrData.getFollowupDate());
				
				row.addColumnValue(new DataSetColumn("weightInKg", "Weight In Kg", String.class), txCurrData.getWeightInKg());
				row.addColumnValue(new DataSetColumn("pregnancyStatus", "Pregnancy Status", String.class),
				    txCurrData.getPregnancyStatus());
				row.addColumnValue(new DataSetColumn("regimen", "Regimen", String.class), txCurrData.getRegimen());
				row.addColumnValue(new DataSetColumn("arvDoseDays", "ARV Dose Days", String.class),
				    txCurrData.getArvDoseDays());
				row.addColumnValue(new DataSetColumn("followUpStatus", "Follow-up Status", String.class),
				    txCurrData.getFollowUpStatus());
				row.addColumnValue(new DataSetColumn("anitiretroviralAdherenceLevel", "Adherence", String.class),
				    txCurrData.getAnitiretroviralAdherenceLevel());
				row.addColumnValue(
				    new DataSetColumn("nextVisitDate", "Next Visit Date", Date.class),
				    txCurrData.getNextVisitDate() != null ? EthiOhriUtil.getEthiopianDate(new java.util.Date(txCurrData
				            .getNextVisitDate().getTime())) : txCurrData.getNextVisitDate());
				row.addColumnValue(new DataSetColumn("dsdCategory", "DSD Category", String.class),
				    txCurrData.getDsdCategory());
				row.addColumnValue(
				    new DataSetColumn("tptStartDate", "TPT Start Date", Date.class),
				    txCurrData.getTptStartDate() != null ? EthiOhriUtil.getEthiopianDate(new java.util.Date(txCurrData
				            .getTptStartDate().getTime())) : txCurrData.getTptStartDate());
				row.addColumnValue(
				    new DataSetColumn("tptCompletedDate", "TPT Completed Date", Date.class),
				    txCurrData.getTptCompletedDate() != null ? EthiOhriUtil.getEthiopianDate(new java.util.Date(txCurrData
				            .getTptCompletedDate().getTime())) : txCurrData.getTptCompletedDate());
				row.addColumnValue(
				    new DataSetColumn("tptDiscontinuedDate", "TPT Discontinued Date", Date.class),
				    txCurrData.getTptDiscontinuedDate() != null ? EthiOhriUtil.getEthiopianDate(new java.util.Date(
				            txCurrData.getTptDiscontinuedDate().getTime())) : txCurrData.getTptDiscontinuedDate());
				row.addColumnValue(
				    new DataSetColumn("tuberculosisTreatmentEndDate", "TB Treatment Completed Date", Date.class),
				    txCurrData.getTuberculosisTreatmentEndDate() != null ? EthiOhriUtil.getEthiopianDate(new java.util.Date(
				            txCurrData.getTuberculosisTreatmentEndDate().getTime())) : txCurrData
				            .getTuberculosisTreatmentEndDate());
				row.addColumnValue(
				    new DataSetColumn("dateViralLoadResultsReceived", "VL Received Date", Date.class),
				    txCurrData.getDateViralLoadResultsReceived() != null ? EthiOhriUtil.getEthiopianDate(new java.util.Date(
				            txCurrData.getDateViralLoadResultsReceived().getTime())) : txCurrData
				            .getDateViralLoadResultsReceived());
				row.addColumnValue(new DataSetColumn("viralLoadTestStatus", "VL Status", String.class),
				    txCurrData.getViralLoadTestStatus());
				row.addColumnValue(new DataSetColumn("VLEligibilityDate", "VL Eligibility Date", Date.class),
				    txCurrData.getVLEligibilityDate());
				
				data.addRow(row);
				
			}
			catch (Exception e) {
				log.info("Exception mapping user dataset definition " + e.getMessage());
			}
			
		}
		return data;
		
	}
	
	private List<TxCurrData> getEtlCurr(TxCurrDataSetDefinitionMamba txCurrDataSetDefinitionMamba) {
        List<TxCurrData> txCurrList = new ArrayList<>();
        DataSource dataSource = ConnectionPoolManager.getInstance().getDataSource();
        try (Connection connection = dataSource.getConnection();
             CallableStatement statement = connection.prepareCall("{call sp_fact_encounter_art_follow_up_tx_curr_query(?)}")) {
            statement.setDate(1, new java.sql.Date(txCurrDataSetDefinitionMamba.getEndDate().getTime()));
            boolean hasResults = statement.execute();

            while (hasResults) {
                try (ResultSet resultSet = statement.getResultSet()) {
                    while (resultSet.next()) { // Iterate through each row
                        TxCurrData data = mapRowToTxCurrData(resultSet);
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
	
	private TxCurrData mapRowToTxCurrData(ResultSet resultSet) throws SQLException {
		return new TxCurrData(resultSet.getString("patient_name"), resultSet.getString("mrn"), resultSet.getString("uan"),
		        resultSet.getInt("current_age"), resultSet.getInt("age_at_enrollment"), resultSet.getString("sex"),
		        resultSet.getString("mobile_no"), resultSet.getDate("hiv_confirmed_date"),
		        resultSet.getDate("art_start_date"), resultSet.getDate("followup_date"), resultSet.getInt("weight_in_kg"),
		        resultSet.getString("pregnancy_status"), resultSet.getString("regimen"),
		        resultSet.getString("arv_dose_days"), resultSet.getString("follow_up_status"),
		        resultSet.getString("anitiretroviral_adherence_level"), resultSet.getDate("next_visit_date"),
		        resultSet.getString("dsd_category"), resultSet.getDate("tpt_start_date"),
		        resultSet.getDate("tpt_completed_date"), resultSet.getDate("tpt_discontinued_date"),
		        resultSet.getDate("tuberculosis_treatment_end_date"), resultSet.getDate("date_viral_load_results_received"),
		        resultSet.getString("viral_load_test_status"), new Date(System.currentTimeMillis()));
	}
}
