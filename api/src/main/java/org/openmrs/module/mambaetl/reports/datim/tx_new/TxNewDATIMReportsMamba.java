package org.openmrs.module.mambaetl.reports.datim.tx_new;

import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_new.BreastFeedingStatusDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_new.FineByAgeAndSexAndCD4DataSetDefinitionMamba;
import org.openmrs.module.mambaetl.datasetdefinition.datim.tx_new.KeyPopulationTypeDataSetDefinitionMamba;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
import org.openmrs.module.mambaetl.helpers.mapper.Cd4Status;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.ReportRequest;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.reporting.report.manager.ReportManager;
import org.openmrs.module.reporting.report.manager.ReportManagerUtil;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

@Component
public class TxNewDATIMReportsMamba implements ReportManager {

    @Override
    public String getUuid() {
        return "872b7ebd-ddc7-41dc-b9be-f3d44ec4ec74";
    } //4d7b385f-331f-400c-8592-f539f4565d9d

    @Override
    public String getName() {
        return "MAMBA DATIM- TX_NEW";
    }

    @Override
    public String getDescription() {
        return "TX new DATIM mamba report ";
    }

    @Override
    public List<Parameter> getParameters() {
        return EthiOhriUtil.getDateRangeParameters();

    }

    @Override
    public ReportDefinition constructReportDefinition() {
        ReportDefinition reportDefinition = new ReportDefinition();
        reportDefinition.setUuid(getUuid());
        reportDefinition.setName(getName());
        reportDefinition.setDescription(getDescription());
        reportDefinition.setParameters(getParameters());


        FineByAgeAndSexAndCD4DataSetDefinitionMamba lessThan200CD4DataSetDefinitionMamba = new FineByAgeAndSexAndCD4DataSetDefinitionMamba();
        lessThan200CD4DataSetDefinitionMamba.addParameters(getParameters());
        lessThan200CD4DataSetDefinitionMamba.setCd4Status(Cd4Status.LOW);
        lessThan200CD4DataSetDefinitionMamba.setDescription("<200 CD4");
        reportDefinition.addDataSetDefinition("<200 CD4",
                EthiOhriUtil.map(lessThan200CD4DataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));

        FineByAgeAndSexAndCD4DataSetDefinitionMamba greaterThan200CD4DataSetDefinitionMamba = new FineByAgeAndSexAndCD4DataSetDefinitionMamba();
        greaterThan200CD4DataSetDefinitionMamba.addParameters(getParameters());
        greaterThan200CD4DataSetDefinitionMamba.setCd4Status(Cd4Status.HIGH);
        greaterThan200CD4DataSetDefinitionMamba.setDescription(">= 200 CD4");
        reportDefinition.addDataSetDefinition(">= 200 CD4",
                EthiOhriUtil.map(greaterThan200CD4DataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));

        FineByAgeAndSexAndCD4DataSetDefinitionMamba unknownCD4DataSetDefinitionMamba = new FineByAgeAndSexAndCD4DataSetDefinitionMamba();
        unknownCD4DataSetDefinitionMamba.addParameters(getParameters());
        unknownCD4DataSetDefinitionMamba.setCd4Status(Cd4Status.UNKNOWN);
        reportDefinition.addDataSetDefinition("Unknown CD4",
                EthiOhriUtil.map(unknownCD4DataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));

        BreastFeedingStatusDataSetDefinitionMamba breastFeedingStatusDataSetDefinitionMamba = new BreastFeedingStatusDataSetDefinitionMamba();
        breastFeedingStatusDataSetDefinitionMamba.addParameters(getParameters());
        reportDefinition.addDataSetDefinition("Disaggregated by Breastfeeding Status at ART Initiation",
                EthiOhriUtil.map(breastFeedingStatusDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));

        KeyPopulationTypeDataSetDefinitionMamba keyPopulationTypeDataSetDefinitionMamba = new KeyPopulationTypeDataSetDefinitionMamba();
        keyPopulationTypeDataSetDefinitionMamba.addParameters(getParameters());
        reportDefinition.addDataSetDefinition("Disaggregated by key population type",
                EthiOhriUtil.map(keyPopulationTypeDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));

        return reportDefinition;
    }

    @Override
    public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {

        ReportDesign design = ReportManagerUtil.createExcelDesign("6d4cc920-e087-4b1c-9af2-287c80c0e0f8", reportDefinition);

        return Collections.singletonList(design);
    }

    @Override
    public List<ReportRequest> constructScheduledRequests(ReportDefinition reportDefinition) {
        return null;
    }

    @Override
    public String getVersion() {
        return "1.0.0-SNAPSHOT";
    }

}
