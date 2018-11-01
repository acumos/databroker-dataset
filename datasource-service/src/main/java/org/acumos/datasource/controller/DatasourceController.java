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

import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.acumos.datasource.common.CmlpApplicationEnum;
import org.acumos.datasource.common.DataSrcErrorList;
import org.acumos.datasource.common.DataSrcRestError;
import org.acumos.datasource.common.ErrorListEnum;
import org.acumos.datasource.common.FilterResults;
import org.acumos.datasource.common.HelperTool;
import org.acumos.datasource.common.JsonResponse;
import org.acumos.datasource.common.ProxyManager;
import org.acumos.datasource.exception.DataSrcException;
import org.acumos.datasource.schema.DataSourceMetadata;
import org.acumos.datasource.schema.DataSourceModel;
import org.acumos.datasource.schema.DataSourceModelPost;
import org.acumos.datasource.schema.DataSourceModelPut;
import org.acumos.datasource.service.DataSourceServiceV2Impl;
import org.acumos.datasource.utils.ApplicationUtilities;
import org.acumos.datasource.utils.GlobalKeys;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
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
@RequestMapping(value = "/"+GlobalKeys.DATASOURCES)
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
	
	@Autowired
	HelperTool helperTool;

	@Autowired
	ApplicationUtilities applicationUtilities;
	
	private HttpHeaders getHeaders(Response rsp) {
		MultivaluedMap<String, Object> mHeader = rsp.getHeaders();
		HttpHeaders headers = new HttpHeaders();
		for(String key: mHeader.keySet()) {
			headers.add(key, mHeader.get(key).toString().replace("[", "").replace("]", ""));
		}
		
		return headers;
	}
	
	@RequestMapping(method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = GlobalKeys.RESPOND_A_LIST_DATASOURCE_CONNECTION_DETAILS, 
				  notes = GlobalKeys.RETURNS_A_JSON_ARRAY_THAT_DENOTES_LIST_OF_ALL_DATASOURCE_CONNECTION_DETAILS_BASED_ON_AUTHORIZATION_AND_SHARE_DATASOURCE_FLAG, response = DataSourceModel.class)
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = GlobalKeys.OK),
			@ApiResponse(code = 401, message = GlobalKeys.UNAUTHORIZED),
			@ApiResponse(code = 403, message = GlobalKeys.FORBIDDEN),
			@ApiResponse(code = 404, message = GlobalKeys.NOT_FOUND),
			@ApiResponse(code = 500, message = GlobalKeys.INTERNAL_SERVER_ERROR) })
	@ResponseBody
	public ResponseEntity<JsonResponse> getDataSourcesList(@RequestHeader("Authorization") String authorization,
			@RequestParam(value=GlobalKeys.NAMESPACE2, required=false) String namespace,
			@RequestParam(value=GlobalKeys.CATEGORY2, required=false) String category,
			@RequestParam(value=GlobalKeys.TEXT_SEARCH, required=false) String textSearch,
			@RequestParam(value=GlobalKeys.PAGE, defaultValue = "0") int pgOffset, 
			@RequestParam(value=GlobalKeys.PER_PAGE, defaultValue = "0") int pgLimit) {
		
		String remoteUser;
		List<String> results = null;
		try {
			log.info("getDataSourcesList, namespace value being passed: " + namespace);
			log.info("getDataSourcesList, category value being passed: " + category);
			log.info("getDataSourcesList, textSearch value being passed: " + textSearch);
			log.info("getDataSourcesList, offset value being passed: " + pgOffset);
			log.info("getDataSourcesList, limit value being passed: " + pgLimit);
			
			String url = helperTool.getResourceURL(request);
			
			if(category != null) {
				applicationUtilities.validateInputParameter("category", category, true);
			}
			
			//check for input values
			if((pgOffset > 0 && pgLimit == 0) || (pgOffset == 0 && pgLimit > 0)) {
				String[] variables = { "page", "perPage" };
				DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
				
				throw new DataSrcException ("Invalid input: Both Offset and Limit values should be either 0s or > 0",
						Status.NOT_FOUND.getStatusCode(), err);
			}

			remoteUser = helperTool.getRemoteUser(request);
			log.info("getDataSourcesList with user boolean as true, remote user detail: " + remoteUser);

			results = service.getDataSourcesList(remoteUser, authorization, namespace, category, null, textSearch);
			log.info("getDataSourcesList with user boolean as true, No. of datasources returning are: "
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
						log.info("getDataSourcesList(), No. of datasources returning are: " + String.valueOf(results.size()));
						
						ResponseBuilder responseBuilder = Response.ok(results.toString());
						Response rsp =  FilterResults.setPaginationRecord(pgOffset, pgLimit, totalRecords, url, responseBuilder);
						return new ResponseEntity<JsonResponse>(new JsonResponse(rsp.getEntity().toString()) , getHeaders(rsp), HttpStatus.OK);
						
					} else if (results.size() > 0 && (pgOffset*pgLimit) >=  totalRecords) {
						
						log.info("getDataSourcesList(), No. of datasources returning are: " + String.valueOf(results.size()));
						ResponseBuilder responseBuilder = Response.ok(results.toString());
						Response rsp = FilterResults.setPaginationRecord(pgOffset, pgLimit, totalRecords, url, responseBuilder);
						return new ResponseEntity<JsonResponse>(new JsonResponse(rsp.getEntity().toString()) , getHeaders(rsp), HttpStatus.OK);
						
					} else {
						
						log.info("getDataSourcesList() requested range exceeds total number of datasources. Returning zero datasources.");
						StringBuilder sb = new StringBuilder();
						sb.append(url).append("?page=").append(1).append("&perPage=").append(pgLimit);
						Response rsp = Response.status(Status.OK.getStatusCode()).link(sb.toString(), "first").build();
						return new ResponseEntity<JsonResponse>(new JsonResponse(rsp.getEntity().toString()) , getHeaders(rsp), HttpStatus.OK);
					}
					
				} else {
					Response rsp =  Response.ok(results.toString()).build();
					return new ResponseEntity<JsonResponse>(new JsonResponse(rsp.getEntity().toString()) , getHeaders(rsp), HttpStatus.OK);
				}
				
			} else if(pgOffset > 0 && pgLimit > 0) { //zero records for the given search, with pagination
				
				log.info("getDataSourcesList() requested range exceeds total number of datasources. Returning zero datasources.");
				StringBuilder sb = new StringBuilder();
				sb.append(url).append("?page=").append(1).append("&perPage=").append(pgLimit);
				
				Response rsp =  Response.status(Status.OK.getStatusCode()).link(sb.toString(), "first").build();
				return new ResponseEntity<JsonResponse>(new JsonResponse(rsp.getEntity().toString()) , getHeaders(rsp), HttpStatus.OK);

			} else { //zero records for the given searce
				return new ResponseEntity<JsonResponse>(new JsonResponse("[]") , HttpStatus.OK);
			}
			
		} catch (DataSrcException cmlpException) {
			Response rsp =   cmlpException.toResponse();
			return new ResponseEntity<JsonResponse>(new JsonResponse(rsp.getEntity().toString()) , getHeaders(rsp), HttpStatus.OK);
		} catch (Exception e) {
			log.info("getDataSourcesList, Unknown Exception : " + e.getMessage());
			DataSrcRestError err = DataSrcErrorList.buildError(e, null, CmlpApplicationEnum.DATASOURCE);
			Response rsp =  Response.status(Status.INTERNAL_SERVER_ERROR.getStatusCode()).entity(err).build();
			return new ResponseEntity<JsonResponse>(new JsonResponse(rsp.getEntity().toString()) , getHeaders(rsp), HttpStatus.OK);
		}
	}
	
	
	@RequestMapping(value = "/{" + GlobalKeys.DATASOURCE_KEY + "}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
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
	public ResponseEntity<JsonResponse> getDataSource(@RequestHeader("Authorization") String authorization,
			@PathVariable (GlobalKeys.DATASOURCE_KEY) String dataSourceKey) {

		String remoteUser;
		List<String> result = null;
		try {
			log.info("getDataSource, dataSourceKey value being passed: " + dataSourceKey);
			
			remoteUser = helperTool.getRemoteUser(request);
			log.info("getDataSource with user boolean as true, remote user detail: " + remoteUser);

			result = service.getDataSourcesList(remoteUser, authorization, null, null, dataSourceKey, null);
			log.info("getDataSource with user boolean as true, No. of datasources returning are: "
					+ String.valueOf(result.size()));

			if (result != null && result.size() > 0) {
				Response rsp = Response.ok(result.toString()).build();
				return new ResponseEntity<JsonResponse>(new JsonResponse(rsp.getEntity().toString()) , getHeaders(rsp), HttpStatus.OK);

			} else {
				String[] variables = { "datasourceKey" };
				DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
				
				throw new DataSrcException("No Datasource information available for the given datasourceKey/Search.",
						Status.NOT_FOUND.getStatusCode(), err);
			}
		} catch (DataSrcException cmlpException) {
			Response rsp =  cmlpException.toResponse();
			return new ResponseEntity<JsonResponse>(new JsonResponse(rsp.getEntity().toString()) , getHeaders(rsp), HttpStatus.OK);

		} catch (Exception e) {
			log.info("getDataSource, Unknown Exception : " + e.getMessage());
			DataSrcRestError err = DataSrcErrorList.buildError(e, null, CmlpApplicationEnum.DATASOURCE);
			
			Response rsp =  Response.status(Status.INTERNAL_SERVER_ERROR.getStatusCode()).entity(err).build();
			return new ResponseEntity<JsonResponse>(new JsonResponse(rsp.getEntity().toString()) , getHeaders(rsp), HttpStatus.OK);
		}
	}

	
	
	@RequestMapping(value = "/{datasourceKey}/contents", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
	@ApiOperation(value = GlobalKeys.RESPOND_AN_INPUTSTREAM_OF_REAULTSET_OF_ASSOCIATED_DATASOURCE, notes = GlobalKeys.RETURNS_A_STRING_THAT_DENOTES_AN_INPUTSTREAM_OF_RESULTSET_OF_ASSOCIATED_DATASOURCE)
	@ApiResponses(value = { @ApiResponse(code = 200, message = GlobalKeys.OK),
			@ApiResponse(code = 401, message = GlobalKeys.UNAUTHORIZED),
			@ApiResponse(code = 403, message = GlobalKeys.FORBIDDEN),
			@ApiResponse(code = 404, message = GlobalKeys.NOT_FOUND),
			@ApiResponse(code = 500, message = GlobalKeys.INTERNAL_SERVER_ERROR) })
	@ResponseBody
	public ResponseEntity<JsonResponse> getDataSourceContents(@RequestHeader(GlobalKeys.AUTHORIZATION2) String authorization,
			@PathVariable("datasourceKey") String datasourceKey,
			@RequestParam(value=GlobalKeys.HDFS_FILENAME, required=false) String hdfsFilename, 
			@RequestParam(value=PROXY_HOST, required=false) String proxyHost,
			@RequestParam(value=PROXY_PORT, defaultValue = PROXY_PORT_DEFAULT) int proxyPort,
			@RequestParam(value=PROXY_USER, required=false) String proxyUsername,
			@RequestParam(value=PROXY_PASS, required=false) String proxyPassword) {
		return new ResponseEntity<JsonResponse>(new JsonResponse("[]") , HttpStatus.OK);
	}
	
	
	
	@RequestMapping(value = "/{" + GlobalKeys.DATASOURCE_KEY + "}/metadata", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
	@ApiOperation(value = GlobalKeys.RESPOND_METADATA_OF_ASSOCIATED_DATASOURCE, notes = GlobalKeys.RETURNS_A_STRING_THAT_DENOTES_METADATA_OF_ASSOCIATED_DATASOURCE)
	@ApiResponses(value = { @ApiResponse(code = 200, message = GlobalKeys.OK),
			@ApiResponse(code = 401, message = GlobalKeys.UNAUTHORIZED),
			@ApiResponse(code = 403, message = GlobalKeys.FORBIDDEN),
			@ApiResponse(code = 404, message = GlobalKeys.NOT_FOUND),
			@ApiResponse(code = 500, message = GlobalKeys.INTERNAL_SERVER_ERROR,
			response = DataSourceMetadata.class )}
			)
	@ResponseBody
	public ResponseEntity<String> getMetadata(@RequestHeader(GlobalKeys.AUTHORIZATION2) String authorization,
			@PathVariable(GlobalKeys.DATASOURCE_KEY) String dataSourceKey) {

		log.info("getMetadata_v2, authorization: " + authorization);
		log.info("getMetadata_v2, dataSourceKey: " + dataSourceKey);
		String remoteUser;
		try {
			remoteUser = helperTool.getRemoteUser(request);
			log.info("Get datasource metadata, remote user detail: " + remoteUser);
			String output = service.getMetadataContents(remoteUser, authorization, dataSourceKey);
			
			if(output != null && output.length() > 0) {
				HttpHeaders headers = new HttpHeaders(); 
				headers.setContentType(MediaType.TEXT_PLAIN); 
				return new ResponseEntity<String>(output, headers, HttpStatus.OK);
			} else {
				log.info("getMetadata_v2, Received null response from service call");
				DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._1004, null, null, CmlpApplicationEnum.DATASOURCE);
				
				HttpHeaders headers = new HttpHeaders(); 
				headers.setContentType(MediaType.TEXT_PLAIN); 
				return new ResponseEntity<String>(err.toString(), headers, HttpStatus.NOT_FOUND);
			}
			
		} catch (DataSrcException cmlpException) {
			HttpHeaders headers = new HttpHeaders(); 
			headers.setContentType(MediaType.TEXT_PLAIN); 
			return new ResponseEntity<String>(cmlpException.getErrorMessage(), headers, HttpStatus.valueOf(cmlpException.getStatusCode()));
			
		} catch (Exception e) {
			log.info("getMetadata_v2, Unknown Exception : " + e.getMessage());
			DataSrcRestError err = DataSrcErrorList.buildError(e, null, CmlpApplicationEnum.DATASOURCE);
			
			HttpHeaders headers = new HttpHeaders(); 
			headers.setContentType(MediaType.TEXT_PLAIN);
			return new ResponseEntity<String>(err.toString(), headers, HttpStatus.INTERNAL_SERVER_ERROR);
		}
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
	public ResponseEntity<JsonResponse> saveDataSourceDetail(@RequestHeader(GlobalKeys.AUTHORIZATION2) String authorization,
			@RequestBody DataSourceModelPost dataSource,
			@RequestParam(value=PROXY_HOST, required=false) String proxyHost,
			@RequestParam(value=PROXY_PORT, defaultValue = PROXY_PORT_DEFAULT) int proxyPort,
			@RequestParam(value=PROXY_USER, required=false) String proxyUsername,
			@RequestParam(value=PROXY_PASS, required=false) String proxyPassword) throws Exception {

		String remoteUser;
		try {
			remoteUser = helperTool.getRemoteUser(request);
			log.info("saveDataSourceDetail, remote user detail: " + remoteUser);
			
			String url = helperTool.getResourceURL(request);
			
			String apiVersion = helperTool.getAPIVersion(request); //initiating API version
			
			log.info("saveDataSourceDetail, Request version : " + apiVersion);
			
			ProxyManager.setUpProxy(proxyHost, proxyPort, proxyUsername, proxyPassword);
			String datasourceKey = service.saveDataSourceDetail(remoteUser, authorization, dataSource);
			ProxyManager.tearDownProxy();
			
			log.info("saveDataSourceDetail, datasourcemodel has been saved.");
			if (datasourceKey != null && datasourceKey.length() > 0) { // success

				JSONObject json = new JSONObject();

				json.put("datasourceKey", datasourceKey);
				json.put("resourceURL", url + "/" + datasourceKey);
				
				URI location = new URI(url + "/" + datasourceKey);

				Response rsp = Response.created(location).entity(json.toString()).build();
				return new ResponseEntity<JsonResponse>(new JsonResponse(rsp.getEntity().toString()) , getHeaders(rsp), HttpStatus.OK);

			}  else {
				applicationUtilities.raiseConnectionFailedException(dataSource.getCategory());
				//never reached this part of code as the above method will throw exception
				throw new Exception("Datasource connection failed. Please check connection parameters.");
			}
				
		} catch (DataSrcException cmlpException) {
			Response rsp = cmlpException.toResponse();
			return new ResponseEntity<JsonResponse>(new JsonResponse(rsp.getEntity().toString()) , getHeaders(rsp), HttpStatus.OK);

		} catch (Exception e) {
			log.info("saveDataSourceDetail, Unknown Exception : " + e.getMessage());
			DataSrcRestError err = DataSrcErrorList.buildError(e, null, CmlpApplicationEnum.DATASOURCE);
			
			Response rsp = Response.status(Status.INTERNAL_SERVER_ERROR.getStatusCode()).entity(err).build();
			return new ResponseEntity<JsonResponse>(new JsonResponse(rsp.getEntity().toString()) , getHeaders(rsp), HttpStatus.OK);
		}
	}
	
	
	
	@RequestMapping(value = "/{" + GlobalKeys.DATASOURCE_KEY + "}", method = RequestMethod.PUT, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = GlobalKeys.RESPOND_STATUS_OF_MODIFYING_A_REGISTERED_DATASOURCE, notes = GlobalKeys.RETURNS_A_JSON_STRING_THAT_NOTIFIES_STATUS_OF_MODIFYING_A_REGISTERED_DATASOURCE, response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = GlobalKeys.OK),
			@ApiResponse(code = 401, message = GlobalKeys.UNAUTHORIZED),
			@ApiResponse(code = 403, message = GlobalKeys.FORBIDDEN),
			@ApiResponse(code = 404, message = GlobalKeys.NOT_FOUND),
			@ApiResponse(code = 500, message = GlobalKeys.INTERNAL_SERVER_ERROR,
			response = DataSourceModel.class) })
	@ResponseBody
	public ResponseEntity<JsonResponse> updateDataSourceDetail(@RequestHeader(GlobalKeys.AUTHORIZATION2) String authorization,
			@PathVariable(GlobalKeys.DATASOURCE_KEY) String dataSourceKey, 
			@RequestBody DataSourceModelPut dataSource,
			@RequestParam(value=PROXY_HOST, required=false) String proxyHost,
			@RequestParam(value=PROXY_PORT, defaultValue = PROXY_PORT_DEFAULT) int proxyPort,
			@RequestParam(value=PROXY_USER, required=false) String proxyUsername,
			@RequestParam(value=PROXY_PASS, required=false) String proxyPassword) {
		
		String remoteUser;
		try {
			remoteUser = helperTool.getRemoteUser(request);
			log.info("updateDataSourceDetail, remote user detail: " + remoteUser);
			
			ProxyManager.setUpProxy(proxyHost, proxyPort, proxyUsername, proxyPassword);
			boolean update = service.updateDataSourceDetail(remoteUser, authorization, dataSourceKey, dataSource);
			ProxyManager.tearDownProxy();
			
			log.info("updateDataSourceDetail, the details for " + dataSourceKey
					+ " was attempeted for update and it resulted in boolean flag " + update);
			
			return getDataSource(authorization, dataSourceKey);
			
		} catch (DataSrcException cmlpException) {
			Response rsp =  cmlpException.toResponse();
			return new ResponseEntity<JsonResponse>(new JsonResponse(rsp.getEntity().toString()) , getHeaders(rsp), HttpStatus.OK);
			
		} catch (Exception e) {
			log.info("updateDataSourceDetail, Unknown Exception : " + e.getMessage());
			DataSrcRestError err = DataSrcErrorList.buildError(e, null, CmlpApplicationEnum.DATASOURCE);
			Response rsp =  Response.status(Status.INTERNAL_SERVER_ERROR.getStatusCode()).entity(err).build();
			return new ResponseEntity<JsonResponse>(new JsonResponse(rsp.getEntity().toString()) , getHeaders(rsp), HttpStatus.OK);
		}
	}

	
	@RequestMapping(value = "/{" + GlobalKeys.DATASOURCE_KEY + "}", method = RequestMethod.DELETE, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = GlobalKeys.RESPOND_STATUS_OF_DELETING_A_REGISTERED_DATASOURCE, notes = GlobalKeys.RETURNS_A_JSON_STRING_THAT_NOTIFIES_STATUS_OF_DELETING_A_REGISTERED_DATASOURCE, response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 204, message = GlobalKeys.NO_CONTENT),
			@ApiResponse(code = 401, message = GlobalKeys.UNAUTHORIZED),
			@ApiResponse(code = 403, message = GlobalKeys.FORBIDDEN),
			@ApiResponse(code = 404, message = GlobalKeys.NOT_FOUND),
			@ApiResponse(code = 500, message = GlobalKeys.INTERNAL_SERVER_ERROR,
			response = DataSourceModel.class) })
	@ResponseBody
	public ResponseEntity<JsonResponse> deleteDataSourceDetail(@RequestHeader(GlobalKeys.AUTHORIZATION2) String authorization,
			@PathVariable(GlobalKeys.DATASOURCE_KEY) String dataSourceKey) {
		
		String remoteUser;
		
		try {
			remoteUser = helperTool.getRemoteUser(request);
			log.info("deleteDataSourceDetail, remote user detail: " + remoteUser);
			boolean isDeleted = service.deleteDataSourceDetail(remoteUser, dataSourceKey);
			log.info("deleteDataSourceDetail, the details for " + dataSourceKey
					+ " was attempeted for delete and it resulted in boolean flag " + isDeleted);

			if (isDeleted) {
				JSONObject json = new JSONObject();

				json.put("status", "Success - Datasource has been successfully deleted.");

				Response rsp =   Response.status(Status.NO_CONTENT.getStatusCode()).entity(json.toString()).build();
				return new ResponseEntity<JsonResponse>(new JsonResponse(rsp.getEntity().toString()) , getHeaders(rsp), HttpStatus.OK);
				
			} else {
				
				throw new Exception ("Unknown Exception occuured while deleting the Datasource.");
			}
			
		} catch (DataSrcException cmlpException) {
			Response rsp =   cmlpException.toResponse();
			return new ResponseEntity<JsonResponse>(new JsonResponse(rsp.getEntity().toString()) , getHeaders(rsp), HttpStatus.OK);
			
		} catch (Exception e) {
			log.info("deleteDataSourceDetail, Unknown Exception : " + e.getMessage());
			DataSrcRestError err = DataSrcErrorList.buildError(e, null, CmlpApplicationEnum.DATASOURCE);
			
			Response rsp =   Response.status(Status.INTERNAL_SERVER_ERROR.getStatusCode()).entity(err).build();
			return new ResponseEntity<JsonResponse>(new JsonResponse(rsp.getEntity().toString()) , getHeaders(rsp), HttpStatus.OK);
		}
	}
	
	
	
	@RequestMapping(value = "/{" + GlobalKeys.DATASOURCE_KEY + "}/status", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = GlobalKeys.RESPOND_AN_INPUTSTREAM_OF_SUCCESS_OR_FAILURE_OF_DATASOURCE_CONNECTION_FOR_THE_GIVEN_DATASOURCE_KEY, notes = GlobalKeys.RETURNS_A_STRING_THAT_DENOTES_SUCCESS_OR_FAILURE_OF_DATASOURCE_CONNECTION, response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = GlobalKeys.OK),
			@ApiResponse(code = 401, message = GlobalKeys.UNAUTHORIZED),
			@ApiResponse(code = 403, message = GlobalKeys.FORBIDDEN),
			@ApiResponse(code = 404, message = GlobalKeys.NOT_FOUND),
			@ApiResponse(code = 500, message = GlobalKeys.INTERNAL_SERVER_ERROR,
			response = DataSourceModel.class) })
	@ResponseBody
	public ResponseEntity<JsonResponse> validateDataSourceConnection(@RequestHeader(GlobalKeys.AUTHORIZATION2) String authorization,
			@PathVariable(GlobalKeys.DATASOURCE_KEY) String datasourceKey) {
		 return new ResponseEntity<JsonResponse>(new JsonResponse("[]") , HttpStatus.OK);
	}
	
	
	@RequestMapping(value = "/{" + GlobalKeys.DATASOURCE_KEY + "}/samples", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
	@ApiOperation(value = GlobalKeys.RESPOND_AN_INPUTSTREAM_OF_REAULTSET_OF_ASSOCIATED_DATASOURCE, notes = GlobalKeys.RETURNS_A_STRING_THAT_DENOTES_INPUTSTREAM_OF_RESULTSET_OF_ASSOCIATED_DATASOURCE, response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = GlobalKeys.OK),
			@ApiResponse(code = 401, message = GlobalKeys.UNAUTHORIZED),
			@ApiResponse(code = 403, message = GlobalKeys.FORBIDDEN),
			@ApiResponse(code = 404, message = GlobalKeys.NOT_FOUND),
			@ApiResponse(code = 500, message = GlobalKeys.INTERNAL_SERVER_ERROR,
			response = String.class) })
	public ResponseEntity<String> getDataSourceSamples(@RequestHeader(GlobalKeys.AUTHORIZATION2) String authorization,
			@PathVariable("datasourceKey") String datasourceKey,
			@RequestParam(value=GlobalKeys.HDFS_FILENAME, required=false) String hdfsFilename) {
		
		String remoteUser;
		
		try {
			remoteUser = helperTool.getRemoteUser(request);
			log.info("getDataSourceSamples_v2, remote user detail: " + remoteUser);
			InputStream in = service.getDataSourceSamples(remoteUser, authorization, datasourceKey,hdfsFilename);
			
			if(in != null) {
				HttpHeaders headers = new HttpHeaders(); 
				headers.setContentType(MediaType.TEXT_PLAIN); 
				return new ResponseEntity<String>(applicationUtilities.getStringResponseFromInputStream(in), headers, HttpStatus.OK);
				
			} else {
				log.info("getDataSourceSamples_v2, Received null response from service call");
				DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._1004, null, null, CmlpApplicationEnum.DATASOURCE);
				
				HttpHeaders headers = new HttpHeaders(); 
				headers.setContentType(MediaType.TEXT_PLAIN); 
				return new ResponseEntity<String>(err.toString(), headers, HttpStatus.NOT_FOUND);
			}
			
		} catch (DataSrcException cmlpException) {
			DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._1004, null, null, CmlpApplicationEnum.DATASOURCE);
			HttpHeaders headers = new HttpHeaders(); 
			headers.setContentType(MediaType.TEXT_PLAIN); 
			return new ResponseEntity<String>(cmlpException.getErrorMessage(), headers, HttpStatus.valueOf(cmlpException.getStatusCode()));
			
		} catch (Exception e) {
			log.info("getDataSourceSamples_v2, Unknown Exception : " + e.getMessage());
			DataSrcRestError err = DataSrcErrorList.buildError(e, null, CmlpApplicationEnum.DATASOURCE);
			
			HttpHeaders headers = new HttpHeaders(); 
			headers.setContentType(MediaType.TEXT_PLAIN);
			return new ResponseEntity<String>(err.toString(), headers, HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}
	
	@RequestMapping(value = "/{" + GlobalKeys.DATASOURCE_KEY + "}/prediction", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = GlobalKeys.RESPOND_WITH_STATUS_OF_TRANSACTION_TO_SAVE_PREDICTION_RESULT, notes = GlobalKeys.RETURNS_A_STATUS, response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = GlobalKeys.CONNECTION_SUCCESS),
			@ApiResponse(code = 400, message = GlobalKeys.BAD_REQUEST_OR_INPUT_PARAMETERS_MISSING),
			@ApiResponse(code = 401, message = GlobalKeys.NOT_AUTHORIZED),
			@ApiResponse(code = 403, message = GlobalKeys.ACCESS_FORBIDDEN),
			@ApiResponse(code = 404, message = GlobalKeys.SERVICE_NOT_AVAILABLE),
			@ApiResponse(code = 500, message = GlobalKeys.UNEXPECTED_RUNTIME_ERROR,
			response = DataSourceModel.class) })
	@ResponseBody
	public ResponseEntity<JsonResponse> writebackPrediction(@RequestHeader(GlobalKeys.AUTHORIZATION2) String authorization,
			@PathVariable(GlobalKeys.DATASOURCE_KEY) String datasourceKey, 
			@RequestParam(value=GlobalKeys.HDFS_FILENAME) String hdfsFilename,
			@RequestParam(value=GlobalKeys.INCLUDES_HEADER) String includesHeader,
			@RequestBody String data,
			@RequestParam(value=PROXY_HOST, required=false) String proxyHost,
			@RequestParam(value=PROXY_PORT, defaultValue = PROXY_PORT_DEFAULT) int proxyPort,
			@RequestParam(value=PROXY_USER, required=false) String proxyUsername,
			@RequestParam(value=PROXY_PASS, required=false) String proxyPassword) {
		
		return new ResponseEntity<JsonResponse>(new JsonResponse("[]") , HttpStatus.OK);
	}
}
