package org.acumos.service.controller;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

import org.acumos.service.schema.DataSetSearchKeys;
import org.acumos.service.schema.DatasetAttributeMetaData;
import org.acumos.service.schema.DatasetDatasourceInfo;
import org.acumos.service.schema.DatasetModel_Post;
import org.acumos.service.schema.DatasetModel_Put;
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
 * Answers REST requests to get, create, update and delete datasets.
 * 
 * Added by the CMLP team in version 1.17.3 as an empty placeholder.
 */
@Controller
@RequestMapping(value = "/datasets", produces = MediaType.APPLICATION_JSON_VALUE)
public class DatasetController {

	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	
	@ApiOperation(value = "Respond a list of dataset details based on DatasetKey or Text Search.", notes = "Returns one or more JSON datasets as per the criteria provided.", response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "A successful response with a message body."),
			@ApiResponse(code = 400, message = "Bad Request"), @ApiResponse(code = 401, message = "Unauthorized"),
			@ApiResponse(code = 403, message = "Forbidden"), @ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Internal Server Error") })
	@RequestMapping(method = RequestMethod.GET)
	@ResponseBody
	public List<String> getDataSetList(@RequestParam("textSearch") String textSearch, @RequestParam("searchKeys") DataSetSearchKeys datasetSearchKeys) {
		logger.debug("getDataSetList ", textSearch);
		return new ArrayList<>();
	}

	@ApiOperation(value = "Respond dataset details based on the DatasetKey.", notes = "Returns JSON String of datset as per the datasetKey provided.", response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "A successful response with a message body"),
			@ApiResponse(code = 400, message = "Bad Request"), @ApiResponse(code = 401, message = "Unauthorized"),
			@ApiResponse(code = 403, message = "Forbidden"), @ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Internal Server Error") })
	@RequestMapping(value = "/{datasetKey}", method = RequestMethod.GET)
	public List<String> getDataSet(@PathVariable("datasetKey") String dataSetKey) {
		logger.debug("getDataSet ", dataSetKey);
		return new ArrayList<>();
	}

	
	@ApiOperation(value = "Respond with saved dataset details.", notes = "Returns a JSON string which provides information after saving the dataset.", response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 201, message = "A successful response and a new resource was created."),
			@ApiResponse(code = 400, message = "Bad Request"), @ApiResponse(code = 401, message = "Unauthorized"),
			@ApiResponse(code = 403, message = "Forbidden"), @ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Internal Server Error") })
	@RequestMapping(method = RequestMethod.POST)
	public List<String> saveDataSetDetail(@RequestBody DatasetModel_Post dataSetObject) {
		logger.debug("saveDataSetDetail ", dataSetObject.toString());
		return new ArrayList<>();
	}

	
	@ApiOperation(value = "Respond with updation of dataset details.", notes = "Returns a HTTP status code to update the existing resource without creating a new resource.", response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "A successful response with a message body"),
			@ApiResponse(code = 400, message = "Bad Request"), @ApiResponse(code = 401, message = "Unauthorized"),
			@ApiResponse(code = 403, message = "Forbidden"), @ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Internal Server Error") })
	@RequestMapping(value = "/{datasetKey}", method = RequestMethod.PUT)
	public List<String> updateDataSetDetail(@PathVariable("datasetKey") String dataSetKey, @RequestBody DatasetModel_Put dataSet) {
		logger.debug("updateDataSetDetail ", dataSetKey);
		return new ArrayList<>();
	}

	
	@ApiOperation(value = "Respond with deletion dataset details.", notes = "Returns a HTTP status code accepting deletion request to delete the existing resource.", response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 204, message = "A successful response with an empty message body."),
			@ApiResponse(code = 400, message = "Bad Request"), @ApiResponse(code = 401, message = "Unauthorized"),
			@ApiResponse(code = 403, message = "Forbidden"), @ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Internal Server Error") })
	@RequestMapping(value = "/{datasetKey}", method = RequestMethod.DELETE)
	public List<String> deleteDataSetDetail(@PathVariable("datasetKey") String dataSetKey) {
		logger.debug("deleteDataSetDetail ", dataSetKey);
		return new ArrayList<>();
	}

	
	@ApiOperation(value = "Respond list of datasources associated with a dataset .", notes = "Returns a JSON Array that denotes datasource  associated with a dataset.", response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "A successful response with a message body"),
			@ApiResponse(code = 400, message = "Bad Request"), @ApiResponse(code = 401, message = "Unauthorized"),
			@ApiResponse(code = 403, message = "Access Forbidden"), @ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Internal Server Error") })
	@RequestMapping(value = "/{datasetKey}/datasources", method = RequestMethod.GET)
	public List<String> getDataSources(@PathVariable("datasetKey") String dataSetKey) {
		logger.debug("getDataSources ", dataSetKey);
		return new ArrayList<>();
	}

	
	@ApiOperation(value = "Respond datasource details  associated with a dataset .", notes = "Returns a JSON String that denotes datasource details associated with a dataset.", response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "A successful response with a message body"),
			@ApiResponse(code = 400, message = "Bad Request"), @ApiResponse(code = 401, message = "Unauthorized"),
			@ApiResponse(code = 403, message = "Forbidden"), @ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Internal Server Error") })
	@RequestMapping(value = "/{datasetKey}/datasources", method = RequestMethod.PUT)
	public List<String> updateDataSourceKey(@PathVariable("datasetKey") String dataSetKey, @RequestBody DatasetDatasourceInfo dataSource) {
		logger.debug("updateDataSourceKey ", dataSetKey);
		return new ArrayList<>();
	}


	@ApiOperation(value = "Returns Attribute MetaData.", notes = "Returns a JSON Array of Attribute MetaData for the given Dataset.", response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "A successful response with a message body"),
			@ApiResponse(code = 400, message = "Bad Request"), @ApiResponse(code = 401, message = "Unauthorized"),
			@ApiResponse(code = 403, message = "Forbidden"), @ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Internal Server Error") })
	@RequestMapping(value = "/{datasetKey}/attributeMetaData", method = RequestMethod.GET)
	public List<String> getAttributeMetaData(@PathVariable("datasetKey") String dataSetKey) {
		logger.debug("getAttributeMetaData ", dataSetKey);
		return new ArrayList<>();
	}

	
	@ApiOperation(value = "Respond contents of datasource associated with dataset.", notes = "Returns a character stream of data from the datsource associated with a dataset.", response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "A successful response with a message body"),
			@ApiResponse(code = 400, message = "Bad Request"), @ApiResponse(code = 401, message = "Unauthorized"),
			@ApiResponse(code = 403, message = "Forbidden"), @ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Internal Server Error") })
	@RequestMapping(value = "/{datasetKey}/attributeMetaData", method = RequestMethod.PUT)
	public List<String> updateAttributeMetaData(@PathVariable("datasetKey") String dataSetKey, @RequestBody DatasetAttributeMetaData attributeMetaData) {
		logger.debug("updateAttributeMetaData ", dataSetKey);
		return new ArrayList<>();
	}
	
}
