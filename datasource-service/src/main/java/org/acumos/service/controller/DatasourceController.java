/*-
 * ===============LICENSE_START=======================================================
 * Acumos
 * ===================================================================================
 * Copyright (C) 2018 AT&T Intellectual Property. All rights reserved.
 * ===================================================================================
 * This Acumos software file is distributed by AT&T
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

import javax.xml.ws.Response;

import org.acumos.service.schema.DataSourceMetadata;
import org.acumos.service.schema.DataSourceModel;
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
@RequestMapping(value = "/"+DatasourceController.DATASOURCES, produces = MediaType.APPLICATION_JSON_VALUE)
public class DatasourceController {

	private static final String RETURNS_A_STRING_THAT_DENOTES_INPUTSTREAM_OF_RESULTSET_OF_ASSOCIATED_DATASOURCE = "Returns a string that denotes inputstream of resultset of associated datasource.";

	private static final String UNEXPECTED_RUNTIME_ERROR = "Unexpected Runtime error";

	private static final String SERVICE_NOT_AVAILABLE = "Service not available";

	private static final String ACCESS_FORBIDDEN = "Access Forbidden";

	private static final String NOT_AUTHORIZED = "Not Authorized";

	private static final String CONNECTION_SUCCESS = "connection:success";

	private static final String BAD_REQUEST_OR_INPUT_PARAMETERS_MISSING = "Bad request or input parameters missing";

	private static final String INCLUDES_HEADER = "includesHeader";

	private static final String DATASOURCE = "datasource";

	private static final String HDFS_FILENAME = "hdfsFilename";

	private static final String DATASOURCE_KEY = "datasourceKey";

	private static final String DATASOURCEKEY2 = "datasourcekey";

	static final String DATASOURCES = "datasources";

	private static final String PER_PAGE = "perPage";

	private static final String PAGE = "page";

	private static final String TEXT_SEARCH = "textSearch";

	private static final String CATEGORY2 = "category";

	private static final String NAMESPACE2 = "namespace";

	private static final String RETURNS_A_STATUS = "Returns a status";

	private static final String RESPOND_WITH_STATUS_OF_TRANSACTION_TO_SAVE_PREDICTION_RESULT = "Respond with status of transaction to save prediction result.";

	private static final String RETURNS_A_STRING_THAT_DENOTES_SUCCESS_OR_FAILURE_OF_DATASOURCE_CONNECTION = "Returns a string that denotes success or failure of datasource connection.";

	private static final String RESPOND_AN_INPUTSTREAM_OF_SUCCESS_OR_FAILURE_OF_DATASOURCE_CONNECTION_FOR_THE_GIVEN_DATASOURCE_KEY = "Respond an inputstream of success or failure of datasource connection for the given datasourceKey";

	private static final String NO_CONTENT = "No Content";

	private static final String RETURNS_A_JSON_STRING_THAT_NOTIFIES_STATUS_OF_DELETING_A_REGISTERED_DATASOURCE = "Returns a JSON string that notifies  status of deleting a registered datasource.";

	private static final String RESPOND_STATUS_OF_DELETING_A_REGISTERED_DATASOURCE = "Respond status of deleting a registered datasource.";

	private static final String RETURNS_A_JSON_STRING_THAT_NOTIFIES_STATUS_OF_MODIFYING_A_REGISTERED_DATASOURCE = "Returns a JSON string that notifies status of modifying a registered datasource.";

	private static final String RESPOND_STATUS_OF_MODIFYING_A_REGISTERED_DATASOURCE = "Respond status of modifying a registered datasource.";

	private static final String RETURNS_A_JSON_STRING_THAT_NOTIFIES_STATUS_OF_REGISTERING_NEW_DATASOURCE = "Returns a JSON string that notifies status of registering new datasource.";

	private static final String RESPOND_WITH_STATUS_OF_REGISTERING_NEW_DATASOURCE = "Respond with status of registering new datasource.";

	private static final String RETURNS_A_STRING_THAT_DENOTES_METADATA_OF_ASSOCIATED_DATASOURCE = "Returns a string that denotes metadata of associated datasource.";

	private static final String RESPOND_METADATA_OF_ASSOCIATED_DATASOURCE = "Respond metadata of associated datasource.";

	private static final String RETURNS_A_STRING_THAT_DENOTES_AN_INPUTSTREAM_OF_RESULTSET_OF_ASSOCIATED_DATASOURCE = "Returns a string that denotes an inputstream of resultset of associated datasource.";

	private static final String RESPOND_AN_INPUTSTREAM_OF_REAULTSET_OF_ASSOCIATED_DATASOURCE = "Respond an inputstream of reaultset of associated datasource.";

	private static final String RETURNS_JSON_DATSETS_AS_PER_THE_DATASET_KEY_PROVIDED = "Returns JSON datsets as per the datasetKey provided.";

	private static final String RESPOND_DATASET_DETAILS_BASED_ON_THE_DATASET_KEY = "Respond dataset details based on the DatasetKey.";

	private static final String INTERNAL_SERVER_ERROR = "Internal Server Error";

	private static final String NOT_FOUND = "Not Found";

	private static final String FORBIDDEN = "Forbidden";

	private static final String UNAUTHORIZED = "Unauthorized";

	private static final String OK = "OK";

	private static final String RETURNS_A_JSON_ARRAY_THAT_DENOTES_LIST_OF_ALL_DATASOURCE_CONNECTION_DETAILS_BASED_ON_AUTHORIZATION_AND_SHARE_DATASOURCE_FLAG = "Returns a JSON Array that denotes list of all datasource connection details based on authorization and share datasource flag.";

	private static final String RESPOND_A_LIST_DATASOURCE_CONNECTION_DETAILS = "Respond a list datasource connection details.";

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
	@ApiOperation(value = RESPOND_A_LIST_DATASOURCE_CONNECTION_DETAILS, 
				  notes = RETURNS_A_JSON_ARRAY_THAT_DENOTES_LIST_OF_ALL_DATASOURCE_CONNECTION_DETAILS_BASED_ON_AUTHORIZATION_AND_SHARE_DATASOURCE_FLAG, response = DataSourceModel.class)
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = OK),
			@ApiResponse(code = 401, message = UNAUTHORIZED),
			@ApiResponse(code = 403, message = FORBIDDEN),
			@ApiResponse(code = 404, message = NOT_FOUND),
			@ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR) })
	@ResponseBody
	public List<DataSourceModel> getDataSourcesList(
													@RequestParam(NAMESPACE2) String namespace, 
													@RequestParam(CATEGORY2) String category,
													@RequestParam(TEXT_SEARCH) String textSearch,
													@RequestParam(PAGE) int offset, 
													@RequestParam(PER_PAGE) int limit) {
		logger.debug("getDataSourcesList ", textSearch);
		return new ArrayList<DataSourceModel>();
	}
	
	
	@RequestMapping(value = "/{datasourcekey}", method = RequestMethod.GET)
	@ApiOperation(value = RESPOND_DATASET_DETAILS_BASED_ON_THE_DATASET_KEY, 
				  notes = RETURNS_JSON_DATSETS_AS_PER_THE_DATASET_KEY_PROVIDED, 
				  response = DataSourceModel.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = OK),
			@ApiResponse(code = 400, message = "Bad Request"),
			@ApiResponse(code = 401, message = UNAUTHORIZED),
			@ApiResponse(code = 403, message = FORBIDDEN),
			@ApiResponse(code = 404, message = NOT_FOUND),
			@ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR) })
	public DataSourceModel getDataSource(@PathVariable (DATASOURCEKEY2) String datasourceKey) {
		logger.debug("getDataSource", datasourceKey);
		return new DataSourceModel();
	}

	
	
	@RequestMapping(value = "/{datasourcekey}/contents", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
	@ApiOperation(value = RESPOND_AN_INPUTSTREAM_OF_REAULTSET_OF_ASSOCIATED_DATASOURCE, notes = RETURNS_A_STRING_THAT_DENOTES_AN_INPUTSTREAM_OF_RESULTSET_OF_ASSOCIATED_DATASOURCE)
	@ApiResponses(value = { @ApiResponse(code = 200, message = OK),
			@ApiResponse(code = 401, message = UNAUTHORIZED),
			@ApiResponse(code = 403, message = FORBIDDEN),
			@ApiResponse(code = 404, message = NOT_FOUND),
			@ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR) })
	public String getDataSourceContents(
			@PathVariable(DATASOURCE_KEY) String dataSourceKey,
			@RequestParam(HDFS_FILENAME) String hdfsFilename, 
			@RequestParam(PROXY_HOST) String proxyHost,
			@RequestParam(PROXY_PORT) int proxyPort,
			@RequestParam(PROXY_USER) String proxyUsername,
			@RequestParam(PROXY_PASS) String proxyPassword) {
		return "";
	}
	
	
	
	@RequestMapping(value = "/{datasourcekey}/metadata", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
	@ApiOperation(value = RESPOND_METADATA_OF_ASSOCIATED_DATASOURCE, notes = RETURNS_A_STRING_THAT_DENOTES_METADATA_OF_ASSOCIATED_DATASOURCE)
	@ApiResponses(value = { @ApiResponse(code = 200, message = OK),
			@ApiResponse(code = 401, message = UNAUTHORIZED),
			@ApiResponse(code = 403, message = FORBIDDEN),
			@ApiResponse(code = 404, message = NOT_FOUND),
			@ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR,
			response = DataSourceMetadata.class )}
			)
	public List<DataSourceMetadata> getMetadata(@PathVariable(DATASOURCE_KEY) String dataSourceKey) {
		return new ArrayList<DataSourceMetadata>();
	}
	
	
	
	@RequestMapping(method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = RESPOND_WITH_STATUS_OF_REGISTERING_NEW_DATASOURCE, notes = RETURNS_A_JSON_STRING_THAT_NOTIFIES_STATUS_OF_REGISTERING_NEW_DATASOURCE, response = String.class)
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = OK),
			@ApiResponse(code = 400, message = "Bad Request"),
			@ApiResponse(code = 401, message = UNAUTHORIZED),
			@ApiResponse(code = 403, message = FORBIDDEN),
			@ApiResponse(code = 404, message = NOT_FOUND),
			@ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR,
			response = DataSourceModel.class) })
	public DataSourceModel saveDataSourceDetail(
			@RequestBody DataSourceModelPost dataSource,
			@RequestParam(PROXY_HOST) String proxyHost,
			@RequestParam(PROXY_PORT) int proxyPort,
			@RequestParam(PROXY_USER) String proxyUsername,
			@RequestParam(PROXY_PASS) String proxyPassword) throws Exception {
		return new DataSourceModel();
	}
	
	
	
	@RequestMapping(value = "/{datasourcekey}", method = RequestMethod.PUT, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = RESPOND_STATUS_OF_MODIFYING_A_REGISTERED_DATASOURCE, notes = RETURNS_A_JSON_STRING_THAT_NOTIFIES_STATUS_OF_MODIFYING_A_REGISTERED_DATASOURCE, response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = OK),
			@ApiResponse(code = 401, message = UNAUTHORIZED),
			@ApiResponse(code = 403, message = FORBIDDEN),
			@ApiResponse(code = 404, message = NOT_FOUND),
			@ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR,
			response = DataSourceModel.class) })
	public DataSourceModel updateDataSourceDetail(
			@PathVariable(DATASOURCE_KEY) String dataSourcekey, @RequestBody DataSourceModelPut dataSource,
			@RequestParam(PROXY_HOST) String proxyHost,
			@RequestParam(PROXY_PORT) int proxyPort,
			@RequestParam(PROXY_USER) String proxyUsername,
			@RequestParam(PROXY_PASS) String proxyPassword) {
		
		return new DataSourceModel();
	}
	
	
	
	@RequestMapping(value = "/{datasourcekey}", method = RequestMethod.DELETE, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = RESPOND_STATUS_OF_DELETING_A_REGISTERED_DATASOURCE, notes = RETURNS_A_JSON_STRING_THAT_NOTIFIES_STATUS_OF_DELETING_A_REGISTERED_DATASOURCE, response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 204, message = NO_CONTENT),
			@ApiResponse(code = 401, message = UNAUTHORIZED),
			@ApiResponse(code = 403, message = FORBIDDEN),
			@ApiResponse(code = 404, message = NOT_FOUND),
			@ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR,
			response = DataSourceModel.class) })
	public DataSourceModel deleteDataSourceDetail(@PathVariable(DATASOURCE_KEY) String datasourceKey) {
		return new DataSourceModel();
	}
	
	
	
	@RequestMapping(value = "/{datasourcekey}/status", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = RESPOND_AN_INPUTSTREAM_OF_SUCCESS_OR_FAILURE_OF_DATASOURCE_CONNECTION_FOR_THE_GIVEN_DATASOURCE_KEY, notes = RETURNS_A_STRING_THAT_DENOTES_SUCCESS_OR_FAILURE_OF_DATASOURCE_CONNECTION, response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = OK),
			@ApiResponse(code = 401, message = UNAUTHORIZED),
			@ApiResponse(code = 403, message = FORBIDDEN),
			@ApiResponse(code = 404, message = NOT_FOUND),
			@ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR,
			response = DataSourceModel.class) })
	public DataSourceModel validateDataSourceConnection(@PathVariable(DATASOURCE_KEY) String datasourceKey) {
		return new DataSourceModel();
	}
	
	
	
	@RequestMapping(value = "/{datasourcekey}/samples", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
	@ApiOperation(value = RESPOND_AN_INPUTSTREAM_OF_REAULTSET_OF_ASSOCIATED_DATASOURCE, notes = RETURNS_A_STRING_THAT_DENOTES_INPUTSTREAM_OF_RESULTSET_OF_ASSOCIATED_DATASOURCE, response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = OK),
			@ApiResponse(code = 401, message = UNAUTHORIZED),
			@ApiResponse(code = 403, message = FORBIDDEN),
			@ApiResponse(code = 404, message = NOT_FOUND),
			@ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR,
			response = String.class) })
	public String getDataSourceSamples(
			@PathVariable(DATASOURCE_KEY) String dataSourceKey, 
			@RequestParam(HDFS_FILENAME) String hdfsFilename) {
		return "";
	}

	
	@RequestMapping(value = "/{datasourcekey}/prediction", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = RESPOND_WITH_STATUS_OF_TRANSACTION_TO_SAVE_PREDICTION_RESULT, notes = RETURNS_A_STATUS, response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = CONNECTION_SUCCESS),
			@ApiResponse(code = 400, message = BAD_REQUEST_OR_INPUT_PARAMETERS_MISSING),
			@ApiResponse(code = 401, message = NOT_AUTHORIZED),
			@ApiResponse(code = 403, message = ACCESS_FORBIDDEN),
			@ApiResponse(code = 404, message = SERVICE_NOT_AVAILABLE),
			@ApiResponse(code = 500, message = UNEXPECTED_RUNTIME_ERROR,
			response = DataSourceModel.class) })
	public DataSourceModel writebackPrediction(
			@PathVariable(DATASOURCE) String dataSourcekey, 
			@RequestParam(HDFS_FILENAME) String hdfsFilename,
			@RequestParam(INCLUDES_HEADER) String includesHeader,
			@RequestBody String data,
			@RequestParam(PROXY_HOST) String proxyHost,
			@RequestParam(PROXY_PORT) int proxyPort,
			@RequestParam(PROXY_USER) String proxyUsername,
			@RequestParam(PROXY_PASS) String proxyPassword) {
		
		return new DataSourceModel();
	}
}
