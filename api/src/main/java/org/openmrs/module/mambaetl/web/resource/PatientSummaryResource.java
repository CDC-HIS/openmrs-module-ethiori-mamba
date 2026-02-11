package org.openmrs.module.mambaetl.web.resource;

import org.openmrs.api.context.Context;
import org.openmrs.module.mambaetl.api.PatientSummaryService;
import org.openmrs.module.webservices.rest.SimpleObject;
import org.openmrs.module.webservices.rest.web.RequestContext;
import org.openmrs.module.webservices.rest.web.RestConstants;
import org.openmrs.module.webservices.rest.web.annotation.Resource;
import org.openmrs.module.webservices.rest.web.resource.api.Searchable;
import org.openmrs.module.webservices.rest.web.resource.impl.EmptySearchResult;
import org.openmrs.module.webservices.rest.web.response.ResponseException;
import java.util.Map;

@Resource(name = RestConstants.VERSION_1 + "/ethiohri-mamba/patientsummary", supportedClass = Object.class, supportedOpenmrsVersions = { "2.0 - 9.*" })
public class PatientSummaryResource implements Searchable {
	
	private PatientSummaryService getService() {
		return Context.getService(PatientSummaryService.class);
	}
	
	@Override
	public SimpleObject search(RequestContext context) throws ResponseException {
		String uuid = context.getRequest().getParameter("patientUuid");
		
		if (uuid == null || uuid.isEmpty()) {
			return new SimpleObject().add("error", "Parameter 'patientUuid' is required");
		}
		
		try {
			Map<String, Object> summary = getService().getPatientSummary(uuid);
			
			if (summary == null) {
				return new EmptySearchResult().toSimpleObject(null);
			}
			
			SimpleObject result = new SimpleObject();
			result.putAll(summary);
			return result;
			
		}
		catch (IllegalArgumentException e) {
			return new SimpleObject().add("error", e.getMessage());
		}
	}
	
	@Override
	public String getUri(Object instance) {
		return null;
	}
}
