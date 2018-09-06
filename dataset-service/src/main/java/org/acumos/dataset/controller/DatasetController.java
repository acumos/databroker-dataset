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

package org.acumos.dataset.controller;

import java.lang.invoke.MethodHandles;

import javax.ws.rs.core.Response;

import org.acumos.dataset.schema.DataSetSearchKeys;
import org.acumos.dataset.schema.DatasetAttributeMetaData;
import org.acumos.dataset.schema.DatasetDatasourceInfo;
import org.acumos.dataset.schema.DatasetModel;
import org.acumos.dataset.schema.DatasetModelPost;
import org.acumos.dataset.schema.DatasetModelPut;
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
 * Answers REST requests to get, create, update and delete datasets.
 * 
 * Added by the CMLP team in version 1.17.3 as an empty placeholder.
 */
@Controller
@RequestMapping(value = "/"+DatasetController.DATASETS, produces = MediaType.APPLICATION_JSON_VALUE)
public class DatasetController {

	private static final String PER_PAGE = "perPage";
	private static final String PAGE = "page";
	static final String DATASETS = "datasets";
	private static final String RETURNS_A_CHARACTER_STREAM_OF_DATA_FROM_THE_DATSOURCE_ASSOCIATED_WITH_A_DATASET = "Returns a character stream of data from the datsource associated with a dataset.";
	private static final String RESPOND_CONTENTS_OF_DATASOURCE_ASSOCIATED_WITH_DATASET = "Respond contents of datasource associated with dataset.";
	private static final String RETURNS_A_JSON_ARRAY_OF_ATTRIBUTE_META_DATA_FOR_THE_GIVEN_DATASET = "Returns a JSON Array of Attribute MetaData for the given Dataset.";
	private static final String RETURNS_ATTRIBUTE_META_DATA = "Returns Attribute MetaData.";
	private static final String RETURNS_A_JSON_STRING_THAT_DENOTES_DATASOURCE_DETAILS_ASSOCIATED_WITH_A_DATASET = "Returns a JSON String that denotes datasource details associated with a dataset.";
	private static final String RESPOND_DATASOURCE_DETAILS_ASSOCIATED_WITH_A_DATASET = "Respond datasource details  associated with a dataset .";
	private static final String ACCESS_FORBIDDEN = "Access Forbidden";
	private static final String A_SUCCESSFUL_RESPONSE_WITH_A_MESSAGE_BODY2 = "A successful response with a message body";
	private static final String RETURNS_A_JSON_ARRAY_THAT_DENOTES_DATASOURCE_ASSOCIATED_WITH_A_DATASET = "Returns a JSON Array that denotes datasource  associated with a dataset.";
	private static final String RESPOND_LIST_OF_DATASOURCES_ASSOCIATED_WITH_A_DATASET = "Respond list of datasources associated with a dataset .";
	private static final String RETURNS_A_HTTP_STATUS_CODE_ACCEPTING_DELETION_REQUEST_TO_DELETE_THE_EXISTING_RESOURCE = "Returns a HTTP status code accepting deletion request to delete the existing resource.";
	private static final String RESPOND_WITH_DELETION_DATASET_DETAILS = "Respond with deletion dataset details.";
	private static final String RETURNS_A_HTTP_STATUS_CODE_TO_UPDATE_THE_EXISTING_RESOURCE_WITHOUT_CREATING_A_NEW_RESOURCE = "Returns a HTTP status code to update the existing resource without creating a new resource.";
	private static final String RESPOND_WITH_UPDATION_OF_DATASET_DETAILS = "Respond with updation of dataset details.";
	private static final String A_SUCCESSFUL_RESPONSE_AND_A_NEW_RESOURCE_WAS_CREATED = "A successful response and a new resource was created.";
	private static final String RETURNS_A_JSON_STRING_WHICH_PROVIDES_INFORMATION_AFTER_SAVING_THE_DATASET = "Returns a JSON string which provides information after saving the dataset.";
	private static final String RESPOND_WITH_SAVED_DATASET_DETAILS = "Respond with saved dataset details.";
	private static final String DATASET_KEY = "datasetKey";
	private static final String RETURNS_JSON_STRING_OF_DATSET_AS_PER_THE_DATASET_KEY_PROVIDED = "Returns JSON String of datset as per the datasetKey provided.";
	private static final String RESPOND_DATASET_DETAILS_BASED_ON_THE_DATASET_KEY = "Respond dataset details based on the DatasetKey.";
	private static final String SEARCH_KEYS = "searchKeys";
	private static final String TEXT_SEARCH = "textSearch";
	private static final String NOT_FOUND = "Not Found";
	private static final String INTERNAL_SERVER_ERROR = "Internal Server Error";
	private static final String FORBIDDEN = "Forbidden";
	private static final String BAD_REQUEST = "Bad Request";
	private static final String UNAUTHORIZED = "Unauthorized";
	private static final String A_SUCCESSFUL_RESPONSE_WITH_A_MESSAGE_BODY = "A successful response with a message body.";
	private static final String RETURNS_ONE_OR_MORE_JSON_DATASETS_AS_PER_THE_CRITERIA_PROVIDED = "Returns one or more JSON datasets as per the criteria provided.";
	private static final String RESPOND_A_LIST_OF_DATASET_DETAILS_BASED_ON_DATASET_KEY_OR_TEXT_SEARCH = "Respond a list of dataset details based on DatasetKey or Text Search.";
	
	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	
	@Autowired
	private RestDatasetSvcImplV2 service;
	
	@ApiOperation(value = RESPOND_A_LIST_OF_DATASET_DETAILS_BASED_ON_DATASET_KEY_OR_TEXT_SEARCH, notes = RETURNS_ONE_OR_MORE_JSON_DATASETS_AS_PER_THE_CRITERIA_PROVIDED, response = DatasetModel.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = A_SUCCESSFUL_RESPONSE_WITH_A_MESSAGE_BODY),
			@ApiResponse(code = 400, message = BAD_REQUEST), @ApiResponse(code = 401, message = UNAUTHORIZED),
			@ApiResponse(code = 403, message = FORBIDDEN), @ApiResponse(code = 404, message = NOT_FOUND),
			@ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR) })
	@RequestMapping(method = RequestMethod.GET)
	@ResponseBody
	public Response getDataSetList(@RequestHeader String authorization, @RequestParam(TEXT_SEARCH) String textSearch, 
								   @RequestParam(SEARCH_KEYS) DataSetSearchKeys datasetSearchKeys,
								   @RequestParam(PAGE) int offset, @RequestParam(PER_PAGE) int limit) {
		logger.debug("getDataSetList ", textSearch);
		return service.getDataSetListV2(authorization, textSearch, datasetSearchKeys, offset, limit);
	}

	@ApiOperation(value = RESPOND_DATASET_DETAILS_BASED_ON_THE_DATASET_KEY, notes = RETURNS_JSON_STRING_OF_DATSET_AS_PER_THE_DATASET_KEY_PROVIDED, response = DatasetModel.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = A_SUCCESSFUL_RESPONSE_WITH_A_MESSAGE_BODY2),
			@ApiResponse(code = 400, message = BAD_REQUEST), @ApiResponse(code = 401, message = UNAUTHORIZED),
			@ApiResponse(code = 403, message = FORBIDDEN), @ApiResponse(code = 404, message = NOT_FOUND),
			@ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR) })
	@RequestMapping(value = "/{datasetKey}", method = RequestMethod.GET)
	@ResponseBody
	public Response getDataSet(@RequestHeader String authorization,@PathVariable(DATASET_KEY) String dataSetKey) {
		logger.debug("getDataSet ", dataSetKey);
		return service.getDataSetV2(authorization, dataSetKey);
	}

	
	@ApiOperation(value = RESPOND_WITH_SAVED_DATASET_DETAILS, notes = RETURNS_A_JSON_STRING_WHICH_PROVIDES_INFORMATION_AFTER_SAVING_THE_DATASET, response = DatasetModel.class)
	@ApiResponses(value = { @ApiResponse(code = 201, message = A_SUCCESSFUL_RESPONSE_AND_A_NEW_RESOURCE_WAS_CREATED),
			@ApiResponse(code = 400, message = BAD_REQUEST), @ApiResponse(code = 401, message = UNAUTHORIZED),
			@ApiResponse(code = 403, message = FORBIDDEN), @ApiResponse(code = 404, message = NOT_FOUND),
			@ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR) })
	@RequestMapping(method = RequestMethod.POST)
	@ResponseBody
	public Response saveDataSetDetail(@RequestHeader String authorization,@RequestBody DatasetModelPost dataSetObject) {
		logger.debug("saveDataSetDetail ", dataSetObject.toString());
		return service.saveDataSetDetailV2(authorization, dataSetObject);
	}

	
	@ApiOperation(value = RESPOND_WITH_UPDATION_OF_DATASET_DETAILS, notes = RETURNS_A_HTTP_STATUS_CODE_TO_UPDATE_THE_EXISTING_RESOURCE_WITHOUT_CREATING_A_NEW_RESOURCE, response = DatasetModel.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = A_SUCCESSFUL_RESPONSE_WITH_A_MESSAGE_BODY2),
			@ApiResponse(code = 400, message = BAD_REQUEST), @ApiResponse(code = 401, message = UNAUTHORIZED),
			@ApiResponse(code = 403, message = FORBIDDEN), @ApiResponse(code = 404, message = NOT_FOUND),
			@ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR) })
	@RequestMapping(value = "/{datasetKey}", method = RequestMethod.PUT)
	@ResponseBody
	public Response updateDataSetDetail(@RequestHeader String authorization,@PathVariable(DATASET_KEY) String dataSetKey, @RequestBody DatasetModelPut dataSet) {
		logger.debug("updateDataSetDetail ", dataSetKey);
		return service.updateDataSetDetailV2(authorization, dataSetKey, dataSet);
	}

	
	@ApiOperation(value = RESPOND_WITH_DELETION_DATASET_DETAILS, notes = RETURNS_A_HTTP_STATUS_CODE_ACCEPTING_DELETION_REQUEST_TO_DELETE_THE_EXISTING_RESOURCE, response = DatasetModel.class)
	@ApiResponses(value = { @ApiResponse(code = 204, message = "A successful response with an empty message body."),
			@ApiResponse(code = 400, message = BAD_REQUEST), @ApiResponse(code = 401, message = UNAUTHORIZED),
			@ApiResponse(code = 403, message = FORBIDDEN), @ApiResponse(code = 404, message = NOT_FOUND),
			@ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR) })
	@RequestMapping(value = "/{datasetKey}", method = RequestMethod.DELETE)
	@ResponseBody
	public Response deleteDataSetDetail(@RequestHeader String authorization,@PathVariable(DATASET_KEY) String dataSetKey) {
		logger.debug("deleteDataSetDetail ", dataSetKey);
		return service.deleteDataSetDetailV2(authorization, dataSetKey);
	}

	
	@ApiOperation(value = RESPOND_LIST_OF_DATASOURCES_ASSOCIATED_WITH_A_DATASET, notes = RETURNS_A_JSON_ARRAY_THAT_DENOTES_DATASOURCE_ASSOCIATED_WITH_A_DATASET, response = DatasetDatasourceInfo.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = A_SUCCESSFUL_RESPONSE_WITH_A_MESSAGE_BODY2),
			@ApiResponse(code = 400, message = BAD_REQUEST), @ApiResponse(code = 401, message = UNAUTHORIZED),
			@ApiResponse(code = 403, message = ACCESS_FORBIDDEN), @ApiResponse(code = 404, message = NOT_FOUND),
			@ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR) })
	@RequestMapping(value = "/{datasetKey}/datasources", method = RequestMethod.GET)
	@ResponseBody
	public Response getDataSources(@RequestHeader String authorization,@PathVariable(DATASET_KEY) String dataSetKey) {
		logger.debug("getDataSources ", dataSetKey);
		return service.getDataSourcesV2(authorization, dataSetKey);
	}

	
	@ApiOperation(value = RESPOND_DATASOURCE_DETAILS_ASSOCIATED_WITH_A_DATASET, notes = RETURNS_A_JSON_STRING_THAT_DENOTES_DATASOURCE_DETAILS_ASSOCIATED_WITH_A_DATASET, response = DatasetDatasourceInfo.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = A_SUCCESSFUL_RESPONSE_WITH_A_MESSAGE_BODY2),
			@ApiResponse(code = 400, message = BAD_REQUEST), @ApiResponse(code = 401, message = UNAUTHORIZED),
			@ApiResponse(code = 403, message = FORBIDDEN), @ApiResponse(code = 404, message = NOT_FOUND),
			@ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR) })
	@RequestMapping(value = "/{datasetKey}/datasources", method = RequestMethod.PUT)
	@ResponseBody
	public Response updateDataSourceKey(@RequestHeader String authorization,@PathVariable(DATASET_KEY) String dataSetKey, @RequestBody DatasetDatasourceInfo dataSource) {
		logger.debug("updateDataSourceKey ", dataSetKey);
		return service.updateDataSourceKeyV2(authorization, dataSetKey, dataSource);
	}


	@ApiOperation(value = RETURNS_ATTRIBUTE_META_DATA, notes = RETURNS_A_JSON_ARRAY_OF_ATTRIBUTE_META_DATA_FOR_THE_GIVEN_DATASET, response = DatasetAttributeMetaData.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = A_SUCCESSFUL_RESPONSE_WITH_A_MESSAGE_BODY2),
			@ApiResponse(code = 400, message = BAD_REQUEST), @ApiResponse(code = 401, message = UNAUTHORIZED),
			@ApiResponse(code = 403, message = FORBIDDEN), @ApiResponse(code = 404, message = NOT_FOUND),
			@ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR) })
	@RequestMapping(value = "/{datasetKey}/attributeMetaData", method = RequestMethod.GET)
	@ResponseBody
	public Response getAttributeMetaData(@RequestHeader String authorization,@PathVariable(DATASET_KEY) String dataSetKey) {
		logger.debug("getAttributeMetaData ", dataSetKey);
		return service.getAttributeMetaDataV2(authorization, dataSetKey);
	}

	
	@ApiOperation(value = RESPOND_CONTENTS_OF_DATASOURCE_ASSOCIATED_WITH_DATASET, notes = RETURNS_A_CHARACTER_STREAM_OF_DATA_FROM_THE_DATSOURCE_ASSOCIATED_WITH_A_DATASET, response = DatasetAttributeMetaData.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = A_SUCCESSFUL_RESPONSE_WITH_A_MESSAGE_BODY2),
			@ApiResponse(code = 400, message = BAD_REQUEST), @ApiResponse(code = 401, message = UNAUTHORIZED),
			@ApiResponse(code = 403, message = FORBIDDEN), @ApiResponse(code = 404, message = NOT_FOUND),
			@ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR) })
	@RequestMapping(value = "/{datasetKey}/attributeMetaData", method = RequestMethod.PUT)
	@ResponseBody
	public Response updateAttributeMetaData(@RequestHeader String authorization,@PathVariable(DATASET_KEY) String dataSetKey, @RequestBody DatasetAttributeMetaData attributeMetaData) {
		logger.debug("updateAttributeMetaData ", dataSetKey);
		return service.updateAttributeMetaDataV2(authorization, dataSetKey, attributeMetaData);
	}
	
}
