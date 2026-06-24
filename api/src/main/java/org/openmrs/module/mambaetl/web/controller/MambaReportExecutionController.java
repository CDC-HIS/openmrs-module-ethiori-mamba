package org.openmrs.module.mambaetl.web.controller;

import org.openmrs.module.mambaetl.service.ReportJobService;
import org.openmrs.module.mambaetl.web.resource.ReportJob;
import org.openmrs.module.webservices.rest.web.RestConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.HashMap;
import java.util.Map;

@Controller
@RequestMapping("/rest/" + RestConstants.VERSION_1 + "/ethiohri-mamba/reports")
public class MambaReportExecutionController {
	
	private static final Log log = LogFactory.getLog(MambaReportExecutionController.class);
	
	@Autowired
	private ReportJobService reportJobService;
	
	@RequestMapping(value = "/submit/{procedureName}", method = RequestMethod.POST, produces = "application/json")
	@ResponseBody
	public ResponseEntity<ReportJob> submitReport(@PathVariable String procedureName,
	        @RequestParam(required = false, defaultValue = "0") int offset,
	        @RequestParam(required = false, defaultValue = "0") int limit,
	        @RequestParam Map<String, String> allRequestParams) {
		Map<String, String> params = new HashMap<>(allRequestParams);
		params.remove("offset");
		params.remove("limit");
		ReportJob job = reportJobService.submitJob(procedureName, params, offset, limit);
		return new ResponseEntity<>(job, HttpStatus.ACCEPTED);
	}
	
	@RequestMapping(value = "/jobs/{jobId}", method = RequestMethod.GET, produces = "application/json")
	@ResponseBody
	public ResponseEntity<ReportJob> getJobStatus(@PathVariable String jobId) {
		ReportJob job = reportJobService.getJob(jobId);
		if (job == null) {
			return new ResponseEntity<>(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<>(job, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/jobs/{jobId}", method = RequestMethod.DELETE)
	@ResponseBody
	public ResponseEntity<Void> cancelJob(@PathVariable String jobId) {
		ReportJob job = reportJobService.getJob(jobId);
		if (job == null) {
			return new ResponseEntity<>(HttpStatus.NOT_FOUND);
		}
		boolean cancelled = reportJobService.cancelJob(jobId);
		if (!cancelled) {
			// Job exists but is already COMPLETE or ERROR — cannot be cancelled
			return new ResponseEntity<>(HttpStatus.CONFLICT);
		}
		return new ResponseEntity<>(HttpStatus.NO_CONTENT);
	}
}
