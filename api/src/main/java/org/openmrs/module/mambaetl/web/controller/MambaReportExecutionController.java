package org.openmrs.module.mambaetl.web.controller;

import org.openmrs.module.mambaetl.service.DynamicReportExecutorService;
import org.openmrs.module.mambaetl.web.resource.ReportDataResponse;
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
import org.springframework.web.bind.annotation.RequestBody;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/rest/" + RestConstants.VERSION_1 + "/ethiohri-mamba/reports")
public class MambaReportExecutionController {
	
	private static final Log log = LogFactory.getLog(MambaReportExecutionController.class);
	
	@Autowired
	private DynamicReportExecutorService reportExecutorService;
	
	@RequestMapping(value = "/execute/{procedureName}", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public ResponseEntity<ReportDataResponse> executeReport(
            @PathVariable String procedureName,
            @RequestParam(required = false, defaultValue = "0") int offset,
            @RequestParam(required = false, defaultValue = "0") int limit,
            @RequestParam Map<String, String> allRequestParams) {

        try {

            List<Map<String, Object>> data = reportExecutorService.executeReport(procedureName, allRequestParams, offset, limit);
            
            ReportDataResponse response = new ReportDataResponse(procedureName, data);
            
            return new ResponseEntity<>(response, HttpStatus.OK);
            
        } catch (IllegalArgumentException e) {
            log.error("Invalid procedure request: " + procedureName, e);
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        } catch (Exception e) {
            log.error("Error executing report procedure: " + procedureName, e);
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
	
	@RequestMapping(value = "/execute/{procedureName}", method = RequestMethod.POST, produces = "application/json", consumes = "application/json")
    @ResponseBody
    public ResponseEntity<ReportDataResponse> executeReportPost(
            @PathVariable String procedureName,
            @RequestParam(required = false, defaultValue = "0") int offset,
            @RequestParam(required = false, defaultValue = "0") int limit,
            @RequestBody Map<String, String> payload) {

        try {
            // executeReport works the same using the payload map instead of url params
            List<Map<String, Object>> data = reportExecutorService.executeReport(procedureName, payload, offset, limit);
            
            ReportDataResponse response = new ReportDataResponse(procedureName, data);
            
            return new ResponseEntity<>(response, HttpStatus.OK);
            
        } catch (IllegalArgumentException e) {
            log.error("Invalid procedure request: " + procedureName, e);
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        } catch (Exception e) {
            log.error("Error executing report procedure: " + procedureName, e);
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
