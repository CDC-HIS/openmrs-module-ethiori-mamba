package org.openmrs.module.mambaetl.reports.linelist;

import org.openmrs.module.mambaetl.datasetdefinition.linelist.ICTContactLineListDataSetDefinitionMamba;
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
public class ICTContactLineListReport implements ReportManager {
    @Override
    public String getUuid() {
        return "9001d52e-1dec-4e34-b1f4-1d22612597f8";
    }

    @Override
    public String getName() {
        return "LINELIST- ICT_CONTACT";
    }

    @Override
    public String getDescription() {
        return "ICT CONTACT";
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

        ICTContactLineListDataSetDefinitionMamba ictContactLineListDataSetDefinitionMamba = new ICTContactLineListDataSetDefinitionMamba();
        ictContactLineListDataSetDefinitionMamba.addParameters(getParameters());
        reportDefinition.addDataSetDefinition("List of ICT Contacts",
                EthiOhriUtil.map(ictContactLineListDataSetDefinitionMamba, "startDate=${startDateGC},endDate=${endDateGC}"));

        return reportDefinition;
    }

    @Override
    public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {

        ReportDesign design = ReportManagerUtil.createExcelDesign("cd346276-2740-44ca-8195-021928d588ee", reportDefinition);
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
