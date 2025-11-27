package org.openmrs.module.mambaetl.reports.linelist;

import org.openmrs.module.mambaetl.datasetdefinition.linelist.HEILineListDatasetDefinition;
import org.openmrs.module.mambaetl.helpers.EthiOhriUtil;
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
public class HEILineListReport implements ReportManager {

    @Override
    public String getUuid() {
        return "a1b2c3d4-e5f6-7890-1234-567890abcdef";
    }

    @Override
    public String getName() {
        return "LINELIST- HEI";
    }

    @Override
    public String getDescription() {
        return "HEI Line List";
    }

    @Override
    public List<Parameter> getParameters() {
        return EthiOhriUtil.getDateRangeParameters(Boolean.TRUE);

    }

    @Override
    public ReportDefinition constructReportDefinition() {
        ReportDefinition reportDefinition = new ReportDefinition();
        reportDefinition.setUuid(getUuid());
        reportDefinition.setName(getName());
        reportDefinition.setDescription(getDescription());
        reportDefinition.setParameters(getParameters());

        HEILineListDatasetDefinition heiLineListDatasetDefinition = new HEILineListDatasetDefinition();
        heiLineListDatasetDefinition.addParameters(getParameters());
        reportDefinition.addDataSetDefinition("List of HEI Clients",
                EthiOhriUtil.map(heiLineListDatasetDefinition, "startDate=${startDateGC},endDate=${endDateGC}"));

        return reportDefinition;
    }

    @Override
    public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {

        ReportDesign design = ReportManagerUtil.createExcelDesign("f1e2d3c4-b5a6-9780-1234-567890abcdef",
                reportDefinition);
        design.setReportDefinition(reportDefinition);

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
