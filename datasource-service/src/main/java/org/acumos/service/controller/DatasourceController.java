/*-
 * ===============LICENSE_START=======================================================
 * Acumos
 * ===================================================================================
 * Copyright (C) 2018 AT&T Intellectual Property & Tech Mahindra. All rights reserved.
 * ===================================================================================
 * This Acumos software file is distributed by AT&T and Tech Mahindra
 * under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ===============LICENSE_END=========================================================
 */

package org.acumos.service.controller;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

import org.acumos.service.schema.DataSourceModelPost;
import org.acumos.service.schema.DataSourceModelPut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

/**
 * Answers REST requests to get, create, update and delete datasources.
 * 
 * Added by the CMLP team in version 1.17.3 as an empty placeholder.
 */
@Controller
@RequestMapping(value = "/datasources", produces = MediaType.APPLICATION_JSON_VALUE)
public class DatasourceController {

	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	
	public static final String PROXY_HOST = "proxyHost";
	public static final String PROXY_PORT = "proxyPort";
	public static final String PROXY_USER = "proxyUser";
	public static final String PROXY_PASS = "proxyPass";
	
	public static final String PROXY_HOST_DEFAULT = "0";
	public static final String PROXY_PORT_DEFAULT = "0";
	public static final String PROXY_USER_DEFAULT = "0";
	public static final String PROXY_PASS_DEFAULT = "0";

	
	@RequestMapping(method = RequestMethod.GET)
	@ApiOperation(value = "Respond a list datasource connection details.", 
				  notes = "Returns a JSON Array that denotes list of all datasource connection details based on authorization and share datasource flag.", response = String.class)
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK"),
			@ApiResponse(code = 401, message = "Unauthorized"),
			@ApiResponse(code = 403, message = "Forbidden"),
			@ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Internal Server Error") })
	@ResponseBody
	public List<String> getDataSourcesList(
			@RequestParam("namespace") String namespace, 
			@RequestParam("category") String category,
			@RequestParam("textSearch") String textSearch,
			@RequestParam("page") int offset, 
			@RequestParam("perPage") int limit) {
		
		logger.debug("getDataSourcesList_v2 ", textSearch);
		return new ArrayList<>();
	}
	
	
	@RequestMapping(value = "/{datasourcekey}", method = RequestMethod.GET)
	@ApiOperation(value = "Respond dataset details based on the DatasetKey.", 
				  notes = "Returns JSON datsets as per the datasetKey provided.", 
				  response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "OK"),
			@ApiResponse(code = 400, message = "Bad Request"),
			@ApiResponse(code = 401, message = "Unauthorized"),
			@ApiResponse(code = 403, message = "Forbidden"),
			@ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Internal Server Error") })
	public List<String> getDataSource(@PathVariable ("datasourcekey") String datasourceKey) {
		
		logger.debug("getDataSource_v2 ", datasourceKey);
		return new ArrayList<>();
	}

	
	
	@RequestMapping(value = "/{datasourcekey}/contents", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
	@ApiOperation(value = "Respond an inputstream of reaultset of associated datasource.", notes = "Returns a string that denotes an inputstream of resultset of associated datasource.")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "OK"),
			@ApiResponse(code = 401, message = "Unauthorized"),
			@ApiResponse(code = 403, message = "Forbidden"),
			@ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Internal Server Error") })
	public List<String> getDataSourceContents(
			@PathVariable("datasourceKey") String dataSourceKey,
			@RequestParam("hdfsFilename") String hdfsFilename, 
			@RequestParam(PROXY_HOST) String proxyHost,
			@RequestParam(PROXY_PORT) int proxyPort,
			@RequestParam(PROXY_USER) String proxyUsername,
			@RequestParam(PROXY_PASS) String proxyPassword) {
		
		return new ArrayList<>();
		
	}
	
	
	
	@RequestMapping(value = "/{datasourcekey}/metadata", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
	@ApiOperation(value = "Respond metadata of associated datasource.", notes = "Returns a string that denotes metadata of associated datasource.")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "OK"),
			@ApiResponse(code = 401, message = "Unauthorized"),
			@ApiResponse(code = 403, message = "Forbidden"),
			@ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Internal Server Error") })
	public List<String> getMetadata(@PathVariable("datasourceKey") String dataSourceKey) {
		
		return new ArrayList<>();
	}
	
	
	
	@RequestMapping(method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Respond with status of registering new datasource.", notes = "Returns a JSON string that notifies status of registering new datasource.", response = String.class)
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK"),
			@ApiResponse(code = 400, message = "Bad Request"),
			@ApiResponse(code = 401, message = "Unauthorized"),
			@ApiResponse(code = 403, message = "Forbidden"),
			@ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Internal Server Error") })
	public List<String> saveDataSourceDetail(
			@RequestBody DataSourceModelPost dataSource,
			@RequestParam(PROXY_HOST) String proxyHost,
			@RequestParam(PROXY_PORT) int proxyPort,
			@RequestParam(PROXY_USER) String proxyUsername,
			@RequestParam(PROXY_PASS) String proxyPassword) throws Exception {
		
		return new ArrayList<>();
	}
	
	
	
	@RequestMapping(value = "/{datasourcekey}", method = RequestMethod.PUT, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Respond status of modifying a registered datasource.", notes = "Returns a JSON string that notifies status of modifying a registered datasource.", response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "OK"),
			@ApiResponse(code = 401, message = "Unauthorized"),
			@ApiResponse(code = 403, message = "Forbidden"),
			@ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Internal Server Error") })
	public List<String> updateDataSourceDetail(
			@PathVariable("datasourceKey") String dataSourcekey, @RequestBody DataSourceModelPut dataSource,
			@RequestParam(PROXY_HOST) String proxyHost,
			@RequestParam(PROXY_PORT) int proxyPort,
			@RequestParam(PROXY_USER) String proxyUsername,
			@RequestParam(PROXY_PASS) String proxyPassword) {
		
		return new ArrayList<>();
	}
	
	
	
	@RequestMapping(value = "/{datasourcekey}", method = RequestMethod.DELETE, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Respond status of deleting a registered datasource.", notes = "Returns a JSON string that notifies  status of deleting a registered datasource.", response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 204, message = "No Content"),
			@ApiResponse(code = 401, message = "Unauthorized"),
			@ApiResponse(code = 403, message = "Forbidden"),
			@ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Internal Server Error") })
	public List<String> deleteDataSourceDetail(
			@PathVariable("datasourceKey") String datasourceKey) {
		
		return new ArrayList<>();
	}
	
	
	
	@RequestMapping(value = "/{datasourcekey}/status", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Respond an inputstream of success or failure of datasource connection for the given datasourceKey", notes = "Returns a string that denotes success or failure of datasource connection.", response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "OK"),
			@ApiResponse(code = 401, message = "Unauthorized"),
			@ApiResponse(code = 403, message = "Forbidden"),
			@ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Internal Server Error") })
	public List<String> validateDataSourceConnection(
			@PathVariable("datasourceKey") String datasourceKey) {
		
		return new ArrayList<>();
	}
	
	
	
	@RequestMapping(value = "/{datasourcekey}/samples", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
	@ApiOperation(value = "Respond an inputstream of reaultset of associated datasource.", notes = "Returns a string that denotes inputstream of resultset of associated datasource.", response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "OK"),
			@ApiResponse(code = 401, message = "Unauthorized"),
			@ApiResponse(code = 403, message = "Forbidden"),
			@ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Internal Server Error") })
	public List<String> getDataSourceSamples(
			@PathVariable("datasourceKey") String dataSourceKey, 
			@RequestParam("hdfsFilename") String hdfsFilename) {
		
		return new ArrayList<>();
	}

	
	@RequestMapping(value = "/{datasourcekey}/prediction", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Respond with status of transaction to save prediction result.", notes = "Returns a status", response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "connection:success"),
			@ApiResponse(code = 400, message = "Bad request or input parameters missing"),
			@ApiResponse(code = 401, message = "Not Authorized"),
			@ApiResponse(code = 403, message = "Access Forbidden"),
			@ApiResponse(code = 404, message = "Service not available"),
			@ApiResponse(code = 500, message = "Unexpected Runtime error") })
	public List<String> writebackPrediction(
			@PathVariable("datasource") String dataSourcekey, 
			@RequestParam("hdfsFilename") String hdfsFilename,
			@RequestParam("includesHeader") String includesHeader,
			@RequestBody String data,
			@RequestParam(PROXY_HOST) String proxyHost,
			@RequestParam(PROXY_PORT) int proxyPort,
			@RequestParam(PROXY_USER) String proxyUsername,
			@RequestParam(PROXY_PASS) String proxyPassword) {
		
		return new ArrayList<>();
	}
}
