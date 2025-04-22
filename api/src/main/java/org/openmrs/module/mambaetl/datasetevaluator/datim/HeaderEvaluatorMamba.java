package org.openmrs.module.mambaetl.datasetevaluator.datim;

import org.openmrs.annotation.Handler;
import org.openmrs.module.mambaetl.datasetdefinition.datim.HeaderDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.mapper.ResultSetMapper;
import org.openmrs.module.reporting.dataset.DataSet;
import org.openmrs.module.reporting.dataset.SimpleDataSet;
import org.openmrs.module.reporting.dataset.definition.DataSetDefinition;
import org.openmrs.module.reporting.dataset.definition.evaluator.DataSetEvaluator;
import org.openmrs.module.reporting.evaluation.EvaluationContext;
import org.openmrs.module.reporting.evaluation.EvaluationException;

@Handler(supports = { HeaderDataSetDefinitionMamba.class })
public class HeaderEvaluatorMamba implements DataSetEvaluator {
	
	@Override
	public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext evalContext) throws EvaluationException {
		HeaderDataSetDefinitionMamba dataSetDefinitionMamba = (HeaderDataSetDefinitionMamba) dataSetDefinition;
		SimpleDataSet data = new SimpleDataSet(dataSetDefinition, evalContext);
		ResultSetMapper resultSetMapper = new ResultSetMapper();
		// Get ResultSet from the database
		
		return data;
	}
	
}
