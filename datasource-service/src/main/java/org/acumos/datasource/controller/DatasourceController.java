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

package org.acumos.datasource.controller;

import java.lang.invoke.MethodHandles;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.acumos.datasource.common.CmlpApplicationEnum;
import org.acumos.datasource.common.CmlpErrorList;
import org.acumos.datasource.common.CmlpRestError;
import org.acumos.datasource.common.ErrorListEnum;
import org.acumos.datasource.common.FilterResults;
import org.acumos.datasource.common.HelperTool;
import org.acumos.datasource.common.Utilities;
import org.acumos.datasource.exception.CmlpDataSrcException;
import org.acumos.datasource.schema.DataSourceMetadata;
import org.acumos.datasource.schema.DataSourceModel;
import org.acumos.datasource.schema.DataSourceModelPost;
import org.acumos.datasource.schema.DataSourceModelPut;
import org.acumos.datasource.service.DataSourceServiceV2Impl;
import org.acumos.datasource.utils.GlobalKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
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
@RequestMapping(value = "/"+GlobalKeys.DATASOURCES, produces = MediaType.APPLICATION_JSON_VALUE)
public class DatasourceController {

	
	

	private static final Logger log= LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	
	public static final String PROXY_HOST = "proxyHost";
	public static final String PROXY_PORT = "proxyPort";
	public static final String PROXY_USER = "proxyUser";
	public static final String PROXY_PASS = "proxyPass";
	
	public static final String PROXY_HOST_DEFAULT = "0";
	public static final String PROXY_PORT_DEFAULT = "0";
	public static final String PROXY_USER_DEFAULT = "0";
	public static final String PROXY_PASS_DEFAULT = "0";
	
	@Autowired
	private DataSourceServiceV2Impl service;
	
	@Autowired
	private HttpServletRequest request;

	
	@RequestMapping(method = RequestMethod.GET)
	@ApiOperation(value = GlobalKeys.RESPOND_A_LIST_DATASOURCE_CONNECTION_DETAILS, 
				  notes = GlobalKeys.RETURNS_A_JSON_ARRAY_THAT_DENOTES_LIST_OF_ALL_DATASOURCE_CONNECTION_DETAILS_BASED_ON_AUTHORIZATION_AND_SHARE_DATASOURCE_FLAG, response = DataSourceModel.class)
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = GlobalKeys.OK),
			@ApiResponse(code = 401, message = GlobalKeys.UNAUTHORIZED),
			@ApiResponse(code = 403, message = GlobalKeys.FORBIDDEN),
			@ApiResponse(code = 404, message = GlobalKeys.NOT_FOUND),
			@ApiResponse(code = 500, message = GlobalKeys.INTERNAL_SERVER_ERROR) })
	@ResponseBody
	public Response getDataSourcesList(@RequestHeader("Authorization") String authorization,
			@RequestParam(value=GlobalKeys.NAMESPACE2, required=false) String namespace,
			@RequestParam(value=GlobalKeys.CATEGORY2, required=false) String category,
			@RequestParam(value=GlobalKeys.TEXT_SEARCH, required=false) String textSearch,
			@RequestParam(value=GlobalKeys.PAGE, defaultValue = "0") int pgOffset, 
			@RequestParam(value=GlobalKeys.PER_PAGE, defaultValue = "0") int pgLimit) {
		
		String remoteUser;
		List<String> results = null;
		try {
			log.info("getDataSourcesList_v2, namespace value being passed: " + namespace);
			log.info("getDataSourcesList_v2, category value being passed: " + category);
			log.info("getDataSourcesList_v2, textSearch value being passed: " + textSearch);
			log.info("getDataSourcesList_v2, offset value being passed: " + pgOffset);
			log.info("getDataSourcesList_v2, limit value being passed: " + pgLimit);
			
			String url = HelperTool.getResourceURL(request);
			
			if(category != null) {
				Utilities.validateInputParameter("category", category, true);
			}
			
			//check for input values
			if((pgOffset > 0 && pgLimit == 0) || (pgOffset == 0 && pgLimit > 0)) {
				String[] variables = { "page", "perPage" };
				CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
				
				throw new CmlpDataSrcException ("Invalid input: Both Offset and Limit values should be either 0s or > 0",
						Status.NOT_FOUND.getStatusCode(), err);
			}

			remoteUser = HelperTool.getRemoteUser(request);
			log.info("getDataSourcesList_v2 with user boolean as true, remote user detail: " + remoteUser);

			results = service.getDataSourcesList(remoteUser, authorization, namespace, category, null, textSearch);
			log.info("getDataSourcesList_v2 with user boolean as true, No. of datasources returning are: "
					+ String.valueOf(results.size()));
			
			int totalRecords = results.size();

			if (results != null && results.size() > 0) {
				
				if(pgOffset == 0 && pgLimit == 0) { //set default
					pgOffset = 1;
					pgLimit = 25;
				}
				
				results = FilterResults.returnDatasets(results, pgOffset, pgLimit);
				
				if(pgOffset > 0 && pgLimit > 0) {
					if(results.size() > 0 && (pgOffset*pgLimit) <  totalRecords) {
						log.info("getDataSourcesList_v2(), No. of datasources returning are: " + String.valueOf(results.size()));
						
						ResponseBuilder responseBuilder = Response.ok(results.toString());
						return FilterResults.setPaginationRecord(pgOffset, pgLimit, totalRecords, url, responseBuilder);
						
					} else if (results.size() > 0 && (pgOffset*pgLimit) >=  totalRecords) {
						
						log.info("getDataSourcesList_v2(), No. of datasources returning are: " + String.valueOf(results.size()));
						
						ResponseBuilder responseBuilder = Response.ok(results.toString());
						return FilterResults.setPaginationRecord(pgOffset, pgLimit, totalRecords, url, responseBuilder);
						
					} else {
						
						log.info("getDataSourcesList_v2() requested range exceeds total number of datasources. Returning zero datasources.");
						
						StringBuffer sb = new StringBuffer();
						sb.append(url).append("?page=").append(1).append("&perPage=").append(pgLimit);
						
						return Response.status(Status.OK.getStatusCode()).link(sb.toString(), "first").build();
					}
					
				} else {
					return Response.ok(results.toString()).build();
				}
				
			} else if(pgOffset > 0 && pgLimit > 0) { //zero records for the given search, with pagination
				
				log.info("getDataSourcesList_v2() requested range exceeds total number of datasources. Returning zero datasources.");
				StringBuffer sb = new StringBuffer();
				sb.append(url).append("?page=").append(1).append("&perPage=").append(pgLimit);
				
				return Response.status(Status.OK.getStatusCode()).link(sb.toString(), "first").build();

			} else { //zero records for the given search
				
				return Response.ok("[]").build();
			}
			
		} catch (CmlpDataSrcException cmlpException) {
			
			return cmlpException.toResponse();
			
		} catch (Exception e) {
			log.info("getDataSourcesList_v2, Unknown Exception : " + e.getMessage());
			CmlpRestError err = CmlpErrorList.buildError(e, null, CmlpApplicationEnum.DATASOURCE);
			
			return Response.status(Status.INTERNAL_SERVER_ERROR.getStatusCode()).entity(err).build();
		}
	}
	
	
	@RequestMapping(value = "/{datasourcekey}", method = RequestMethod.GET)
	@ApiOperation(value = GlobalKeys.RESPOND_DATASET_DETAILS_BASED_ON_THE_DATASET_KEY, 
				  notes = GlobalKeys.RETURNS_JSON_DATSETS_AS_PER_THE_DATASET_KEY_PROVIDED, 
				  response = DataSourceModel.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = GlobalKeys.OK),
			@ApiResponse(code = 400, message = GlobalKeys.BAD_REQUEST),
			@ApiResponse(code = 401, message = GlobalKeys.UNAUTHORIZED),
			@ApiResponse(code = 403, message = GlobalKeys.FORBIDDEN),
			@ApiResponse(code = 404, message = GlobalKeys.NOT_FOUND),
			@ApiResponse(code = 500, message = GlobalKeys.INTERNAL_SERVER_ERROR) })
	@ResponseBody
	public Response getDataSource(@RequestHeader("Authorization") String authorization,
			@PathVariable (GlobalKeys.DATASOURCEKEY2) String dataSourceKey) {

		String remoteUser;
		List<String> result = null;
		try {
			log.info("getDataSource_v2, dataSourceKey value being passed: " + dataSourceKey);
			
			remoteUser = HelperTool.getRemoteUser(request);
			log.info("getDataSource_v2 with user boolean as true, remote user detail: " + remoteUser);

			result = service.getDataSourcesList(remoteUser, authorization, null, null, dataSourceKey, null);
			log.info("getDataSource_v2 with user boolean as true, No. of datasources returning are: "
					+ String.valueOf(result.size()));

			if (result != null && result.size() > 0) {
				return Response.ok(result.toString()).build();

			} else {
				String[] variables = { "datasourceKey" };
				
				CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
				
				throw new CmlpDataSrcException("No Datasource information available for the given datasourceKey/Search.",
						Status.NOT_FOUND.getStatusCode(), err);
			}
		} catch (CmlpDataSrcException cmlpException) {
			return cmlpException.toResponse();

		} catch (Exception e) {
			log.info("getDataSource_v2, Unknown Exception : " + e.getMessage());
			CmlpRestError err = CmlpErrorList.buildError(e, null, CmlpApplicationEnum.DATASOURCE);
			
			return Response.status(Status.INTERNAL_SERVER_ERROR.getStatusCode()).entity(err).build();
		}
	}

	
	
	@RequestMapping(value = "/{datasourcekey}/contents", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
	@ApiOperation(value = GlobalKeys.RESPOND_AN_INPUTSTREAM_OF_REAULTSET_OF_ASSOCIATED_DATASOURCE, notes = GlobalKeys.RETURNS_A_STRING_THAT_DENOTES_AN_INPUTSTREAM_OF_RESULTSET_OF_ASSOCIATED_DATASOURCE)
	@ApiResponses(value = { @ApiResponse(code = 200, message = GlobalKeys.OK),
			@ApiResponse(code = 401, message = GlobalKeys.UNAUTHORIZED),
			@ApiResponse(code = 403, message = GlobalKeys.FORBIDDEN),
			@ApiResponse(code = 404, message = GlobalKeys.NOT_FOUND),
			@ApiResponse(code = 500, message = GlobalKeys.INTERNAL_SERVER_ERROR) })
	@ResponseBody
	public Response getDataSourceContents(@RequestHeader(GlobalKeys.AUTHORIZATION2) String authorization,
			@RequestHeader(GlobalKeys.CODE_CLOUD_AUTHORIZATION) String codeCloudAuthorization,
			@PathVariable(GlobalKeys.DATASOURCE_KEY) String dataSourceKey,
			@RequestParam(GlobalKeys.HDFS_FILENAME) String hdfsFilename, 
			@RequestParam(PROXY_HOST) String proxyHost,
			@RequestParam(PROXY_PORT) int proxyPort,
			@RequestParam(PROXY_USER) String proxyUsername,
			@RequestParam(PROXY_PASS) String proxyPassword) {

		return Response.ok("").build();
	}
	
	
	
	@RequestMapping(value = "/{datasourcekey}/metadata", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
	@ApiOperation(value = GlobalKeys.RESPOND_METADATA_OF_ASSOCIATED_DATASOURCE, notes = GlobalKeys.RETURNS_A_STRING_THAT_DENOTES_METADATA_OF_ASSOCIATED_DATASOURCE)
	@ApiResponses(value = { @ApiResponse(code = 200, message = GlobalKeys.OK),
			@ApiResponse(code = 401, message = GlobalKeys.UNAUTHORIZED),
			@ApiResponse(code = 403, message = GlobalKeys.FORBIDDEN),
			@ApiResponse(code = 404, message = GlobalKeys.NOT_FOUND),
			@ApiResponse(code = 500, message = GlobalKeys.INTERNAL_SERVER_ERROR,
			response = DataSourceMetadata.class )}
			)
	@ResponseBody
	public Response getMetadata(@RequestHeader(GlobalKeys.AUTHORIZATION2) String authorization,
			@RequestHeader(GlobalKeys.CODE_CLOUD_AUTHORIZATION) String codeCloudAuthorization,
			@PathVariable(GlobalKeys.DATASOURCE_KEY) String dataSourceKey) {

		return Response.ok("").build();
	}
	
	
	
	@RequestMapping(method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = GlobalKeys.RESPOND_WITH_STATUS_OF_REGISTERING_NEW_DATASOURCE, notes = GlobalKeys.RETURNS_A_JSON_STRING_THAT_NOTIFIES_STATUS_OF_REGISTERING_NEW_DATASOURCE, response = String.class)
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = GlobalKeys.OK),
			@ApiResponse(code = 400, message = GlobalKeys.BAD_REQUEST),
			@ApiResponse(code = 401, message = GlobalKeys.UNAUTHORIZED),
			@ApiResponse(code = 403, message = GlobalKeys.FORBIDDEN),
			@ApiResponse(code = 404, message = GlobalKeys.NOT_FOUND),
			@ApiResponse(code = 500, message = GlobalKeys.INTERNAL_SERVER_ERROR,
			response = DataSourceModel.class) })
	@ResponseBody
	public Response saveDataSourceDetail(@RequestHeader(GlobalKeys.AUTHORIZATION2) String authorization,
			@RequestHeader(GlobalKeys.CODE_CLOUD_AUTHORIZATION) String codeCloudAuthorization,
			@RequestBody DataSourceModelPost dataSource,
			@RequestParam(PROXY_HOST) String proxyHost,
			@RequestParam(PROXY_PORT) int proxyPort,
			@RequestParam(PROXY_USER) String proxyUsername,
			@RequestParam(PROXY_PASS) String proxyPassword) throws Exception {

		return Response.ok("").build();
	}
	
	
	
	@RequestMapping(value = "/{datasourcekey}", method = RequestMethod.PUT, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = GlobalKeys.RESPOND_STATUS_OF_MODIFYING_A_REGISTERED_DATASOURCE, notes = GlobalKeys.RETURNS_A_JSON_STRING_THAT_NOTIFIES_STATUS_OF_MODIFYING_A_REGISTERED_DATASOURCE, response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = GlobalKeys.OK),
			@ApiResponse(code = 401, message = GlobalKeys.UNAUTHORIZED),
			@ApiResponse(code = 403, message = GlobalKeys.FORBIDDEN),
			@ApiResponse(code = 404, message = GlobalKeys.NOT_FOUND),
			@ApiResponse(code = 500, message = GlobalKeys.INTERNAL_SERVER_ERROR,
			response = DataSourceModel.class) })
	@ResponseBody
	public Response updateDataSourceDetail(@RequestHeader(GlobalKeys.AUTHORIZATION2) String authorization,
			@RequestHeader(GlobalKeys.CODE_CLOUD_AUTHORIZATION) String codeCloudAuthorization,
			@PathVariable(GlobalKeys.DATASOURCE_KEY) String dataSourcekey, 
			@RequestBody DataSourceModelPut dataSource,
			@RequestParam(PROXY_HOST) String proxyHost,
			@RequestParam(PROXY_PORT) int proxyPort,
			@RequestParam(PROXY_USER) String proxyUsername,
			@RequestParam(PROXY_PASS) String proxyPassword) {
		
		return Response.ok("").build();
	}
	
	
	
	@RequestMapping(value = "/{datasourcekey}", method = RequestMethod.DELETE, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = GlobalKeys.RESPOND_STATUS_OF_DELETING_A_REGISTERED_DATASOURCE, notes = GlobalKeys.RETURNS_A_JSON_STRING_THAT_NOTIFIES_STATUS_OF_DELETING_A_REGISTERED_DATASOURCE, response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 204, message = GlobalKeys.NO_CONTENT),
			@ApiResponse(code = 401, message = GlobalKeys.UNAUTHORIZED),
			@ApiResponse(code = 403, message = GlobalKeys.FORBIDDEN),
			@ApiResponse(code = 404, message = GlobalKeys.NOT_FOUND),
			@ApiResponse(code = 500, message = GlobalKeys.INTERNAL_SERVER_ERROR,
			response = DataSourceModel.class) })
	@ResponseBody
	public Response deleteDataSourceDetail(@RequestHeader(GlobalKeys.AUTHORIZATION2) String authorization,
			@PathVariable(GlobalKeys.DATASOURCE_KEY) String datasourceKey) {

		return Response.ok("").build();
	}
	
	
	
	@RequestMapping(value = "/{datasourcekey}/status", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = GlobalKeys.RESPOND_AN_INPUTSTREAM_OF_SUCCESS_OR_FAILURE_OF_DATASOURCE_CONNECTION_FOR_THE_GIVEN_DATASOURCE_KEY, notes = GlobalKeys.RETURNS_A_STRING_THAT_DENOTES_SUCCESS_OR_FAILURE_OF_DATASOURCE_CONNECTION, response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = GlobalKeys.OK),
			@ApiResponse(code = 401, message = GlobalKeys.UNAUTHORIZED),
			@ApiResponse(code = 403, message = GlobalKeys.FORBIDDEN),
			@ApiResponse(code = 404, message = GlobalKeys.NOT_FOUND),
			@ApiResponse(code = 500, message = GlobalKeys.INTERNAL_SERVER_ERROR,
			response = DataSourceModel.class) })
	@ResponseBody
	public Response validateDataSourceConnection(@RequestHeader(GlobalKeys.AUTHORIZATION2) String authorization,
			@RequestHeader(GlobalKeys.CODE_CLOUD_AUTHORIZATION) String codeCloudAuthorization,
			@PathVariable(GlobalKeys.DATASOURCE_KEY) String datasourceKey) {

		return Response.ok("").build();
	}
	
	
	
	@RequestMapping(value = "/{datasourcekey}/samples", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
	@ApiOperation(value = GlobalKeys.RESPOND_AN_INPUTSTREAM_OF_REAULTSET_OF_ASSOCIATED_DATASOURCE, notes = GlobalKeys.RETURNS_A_STRING_THAT_DENOTES_INPUTSTREAM_OF_RESULTSET_OF_ASSOCIATED_DATASOURCE, response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = GlobalKeys.OK),
			@ApiResponse(code = 401, message = GlobalKeys.UNAUTHORIZED),
			@ApiResponse(code = 403, message = GlobalKeys.FORBIDDEN),
			@ApiResponse(code = 404, message = GlobalKeys.NOT_FOUND),
			@ApiResponse(code = 500, message = GlobalKeys.INTERNAL_SERVER_ERROR,
			response = String.class) })
	@ResponseBody
	public Response getDataSourceSamples(@RequestHeader(GlobalKeys.AUTHORIZATION2) String authorization,
			@RequestHeader(GlobalKeys.CODE_CLOUD_AUTHORIZATION) String codeCloudAuthorization,
			@PathVariable(GlobalKeys.DATASOURCE_KEY) String dataSourceKey, 
			@RequestParam(GlobalKeys.HDFS_FILENAME) String hdfsFilename) {

		return Response.ok("").build();
	}

	
	@RequestMapping(value = "/{datasourcekey}/prediction", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = GlobalKeys.RESPOND_WITH_STATUS_OF_TRANSACTION_TO_SAVE_PREDICTION_RESULT, notes = GlobalKeys.RETURNS_A_STATUS, response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = GlobalKeys.CONNECTION_SUCCESS),
			@ApiResponse(code = 400, message = GlobalKeys.BAD_REQUEST_OR_INPUT_PARAMETERS_MISSING),
			@ApiResponse(code = 401, message = GlobalKeys.NOT_AUTHORIZED),
			@ApiResponse(code = 403, message = GlobalKeys.ACCESS_FORBIDDEN),
			@ApiResponse(code = 404, message = GlobalKeys.SERVICE_NOT_AVAILABLE),
			@ApiResponse(code = 500, message = GlobalKeys.UNEXPECTED_RUNTIME_ERROR,
			response = DataSourceModel.class) })
	@ResponseBody
	public Response writebackPrediction(@RequestHeader(GlobalKeys.AUTHORIZATION2) String authorization,
			@PathVariable(GlobalKeys.DATASOURCE) String dataSourcekey, 
			@RequestParam(GlobalKeys.HDFS_FILENAME) String hdfsFilename,
			@RequestParam(GlobalKeys.INCLUDES_HEADER) String includesHeader,
			@RequestBody String data,
			@RequestParam(PROXY_HOST) String proxyHost,
			@RequestParam(PROXY_PORT) int proxyPort,
			@RequestParam(PROXY_USER) String proxyUsername,
			@RequestParam(PROXY_PASS) String proxyPassword) {
		
		return Response.ok("").build();
	}
}
