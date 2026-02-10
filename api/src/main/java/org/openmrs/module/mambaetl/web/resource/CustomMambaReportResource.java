package org.openmrs.module.mambaetl.web.resource;

/**
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 * <p>
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.openmrs.api.context.Context;
import org.openmrs.module.mambaetl.api.CustomMambaReportService;
import org.openmrs.module.mambaetl.api.criteria.CustomMambaReportCriteria;
import org.openmrs.module.mambaetl.api.model.CustomMambaReportItem;
import org.openmrs.module.mambaetl.api.model.CustomMambaReportPagination;
import org.openmrs.module.mambaetl.web.controller.CustomMambaReportRestController;
import org.openmrs.module.webservices.rest.SimpleObject;
import org.openmrs.module.webservices.rest.web.RequestContext;
import org.openmrs.module.webservices.rest.web.RestConstants;
import org.openmrs.module.webservices.rest.web.annotation.Resource;
import org.openmrs.module.webservices.rest.web.resource.api.Searchable;
import org.openmrs.module.webservices.rest.web.resource.impl.EmptySearchResult;
import org.openmrs.module.webservices.rest.web.response.ResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Enumeration;
import java.util.List;

@Resource(name = RestConstants.VERSION_1 + CustomMambaReportRestController.MAMBA_REPORT_REST_NAMESPACE + "/report", supportedClass = CustomMambaReportItem.class, supportedOpenmrsVersions = { "2.0 - 9.*" })
public class CustomMambaReportResource implements Searchable {
	
	private static final Logger log = LoggerFactory.getLogger(CustomMambaReportResource.class);
	
	// Optimization: Reuse ObjectMapper
	private static final ObjectMapper objectMapper = new ObjectMapper();
	
	private CustomMambaReportService getService() {
		return Context.getService(CustomMambaReportService.class);
	}
	
	@Override
	public SimpleObject search(RequestContext context) throws ResponseException {
		
		// Note: This overrides standard REST 'limit' params.
		// Ensure this is intended behavior for your custom report.
		context.setLimit(10);
		context.setStartIndex(0);
		
		// This now works because we added the no-args constructor
		CustomMambaReportCriteria searchCriteria = new CustomMambaReportCriteria();
		
		Integer pageSize = searchCriteria.getPageSize(); // defaults
		Integer pageNumber = searchCriteria.getPageNumber(); // defaults
		
		Enumeration<String> parameterNames = context.getRequest().getParameterNames();
		while (parameterNames.hasMoreElements()) {
			
			String paramName = parameterNames.nextElement();
			String paramValue = context.getRequest().getParameter(paramName);
			
			log.debug("Search API hit with param: {} value: {}", paramName, paramValue);
			
			switch (paramName) {
				case "report_id":
					searchCriteria.setReportId(paramValue);
					break;
				case "page_number":
					try {
						pageNumber = Integer.parseInt(paramValue);
						searchCriteria.setPageNumber(pageNumber);
					}
					catch (NumberFormatException e) {
						log.warn("Invalid page_number format: {}", paramValue);
					}
					break;
				case "page_size":
					try {
						pageSize = Integer.parseInt(paramValue);
						searchCriteria.setPageSize(pageSize);
					}
					catch (NumberFormatException e) {
						log.warn("Invalid page_size format: {}", paramValue);
					}
					break;
				default:
					// This handles your "one parameter from request" requirement dynamically
					searchCriteria.addParameter(paramName, paramValue);
					break;
			}
		}
		
		if (searchCriteria.getReportId() == null) {
			return new EmptySearchResult().toSimpleObject(null);
		}
		
		try {
			if (log.isDebugEnabled()) {
				String argumentsJson = objectMapper.writeValueAsString(searchCriteria);
				log.debug("Mamba search criteria: {}", argumentsJson);
			}
		}
		catch (JsonProcessingException e) {
			log.error("Error serializing search criteria for logging", e);
		}
		
		// Ensure your Service Interface has this method defined
		List<CustomMambaReportItem> mambaReportItems = getService().getMambaReportByCriteria(searchCriteria);
		
		Integer totalRecords = getService().getMambaReportSize(searchCriteria);
		
		log.debug("Total records: {}, Fetched records: {}", totalRecords, mambaReportItems.size());
		
		Integer totalPages = (int) Math.ceil((double) totalRecords / pageSize);
		
		CustomMambaReportPagination pagination = new CustomMambaReportPagination();
		pagination.setPageNumber(pageNumber);
		pagination.setPageSize(pageSize);
		pagination.setTotalRecords(totalRecords);
		pagination.setTotalPages(totalPages);
		
		return new SimpleObject().add("results", mambaReportItems).add("pagination", pagination);
	}
	
	@Override
	public String getUri(Object o) {
		return null;
	}
}
