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

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.acumos.service.schema.DataSetSearchKeys;
import org.acumos.service.schema.DatasetAttributeMetaData;
import org.acumos.service.schema.DatasetDatasourceInfo;
import org.acumos.service.schema.DatasetModelPost;
import org.acumos.service.schema.DatasetModelPut;
import org.springframework.web.bind.annotation.RequestBody;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;


@Api(value = "DataSets")
@Path("datasets")
@Produces({ MediaType.APPLICATION_JSON })
public interface RestDatasetSvcV2 {

	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	@ApiOperation(value = "Respond a list of dataset details based on DatasetKey or Text Search.", notes = "Returns one or more JSON datasets as per the criteria provided.", response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "A successful response with a message body."),
			@ApiResponse(code = 400, message = "Bad Request"), @ApiResponse(code = 401, message = "Unauthorized"),
			@ApiResponse(code = 403, message = "Forbidden"), @ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Internal Server Error") })
	public Response getDataSetListV2(@HeaderParam("Authorization") String authorization,
			@QueryParam("textSearch") String textSearch, @QueryParam("searchKeys") DataSetSearchKeys datasetSearchKeys,
			@QueryParam("page") int offset, @QueryParam("perPage") int limit);

	@GET
	@Path("/{datasetKey}")
	@Produces({ MediaType.APPLICATION_JSON })
	@ApiOperation(value = "Respond dataset details based on the DatasetKey.", notes = "Returns JSON String of datset as per the datasetKey provided.", response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "A successful response with a message body"),
			@ApiResponse(code = 400, message = "Bad Request"), @ApiResponse(code = 401, message = "Unauthorized"),
			@ApiResponse(code = 403, message = "Forbidden"), @ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Internal Server Error") })
	public Response getDataSetV2(@HeaderParam("Authorization") String authorization,
			@PathParam("datasetKey") String dataSetKey);

	@POST
	@Produces({ MediaType.APPLICATION_JSON })
	@ApiOperation(value = "Respond with saved dataset details.", notes = "Returns a JSON string which provides information after saving the dataset.", response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 201, message = "A successful response and a new resource was created."),
			@ApiResponse(code = 400, message = "Bad Request"), @ApiResponse(code = 401, message = "Unauthorized"),
			@ApiResponse(code = 403, message = "Forbidden"), @ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Internal Server Error") })
	public Response saveDataSetDetailV2(@HeaderParam("Authorization") String authorization,
			@RequestBody DatasetModelPost dataSetObject);

	@PUT
	@Path("/{datasetKey}")
	@Produces({ MediaType.APPLICATION_JSON })
	@ApiOperation(value = "Respond with updation of dataset details.", notes = "Returns a HTTP status code to update the existing resource without creating a new resource.", response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "A successful response with a message body"),
			@ApiResponse(code = 400, message = "Bad Request"), @ApiResponse(code = 401, message = "Unauthorized"),
			@ApiResponse(code = 403, message = "Forbidden"), @ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Internal Server Error") })
	public Response updateDataSetDetailV2(@HeaderParam("Authorization") String authorization,
			@PathParam("datasetKey") String dataSetKey, @RequestBody DatasetModelPut dataSet);

	@DELETE
	@Path("/{datasetKey}")
	@Produces({ MediaType.APPLICATION_JSON })
	@ApiOperation(value = "Respond with deletion dataset details.", notes = "Returns a HTTP status code accepting deletion request to delete the existing resource.", response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 204, message = "A successful response with an empty message body."),
			@ApiResponse(code = 400, message = "Bad Request"), @ApiResponse(code = 401, message = "Unauthorized"),
			@ApiResponse(code = 403, message = "Forbidden"), @ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Internal Server Error") })
	public Response deleteDataSetDetailV2(@HeaderParam("Authorization") String authorization,
			@PathParam("datasetKey") String dataSetKey);

	@GET
	@Path("/{datasetKey}/datasources")
	@Produces({ MediaType.APPLICATION_JSON })
	@ApiOperation(value = "Respond list of datasources associated with a dataset .", notes = "Returns a JSON Array that denotes datasource  associated with a dataset.", response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "A successful response with a message body"),
			@ApiResponse(code = 400, message = "Bad Request"), @ApiResponse(code = 401, message = "Unauthorized"),
			@ApiResponse(code = 403, message = "Access Forbidden"), @ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Internal Server Error") })
	public Response getDataSourcesV2(@HeaderParam("Authorization") String authorization,
			@PathParam("datasetKey") String dataSetKey);

	@PUT
	@Path("/{datasetKey}/datasources")
	@Produces({ MediaType.APPLICATION_JSON })
	@ApiOperation(value = "Respond datasource details  associated with a dataset .", notes = "Returns a JSON String that denotes datasource details associated with a dataset.", response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "A successful response with a message body"),
			@ApiResponse(code = 400, message = "Bad Request"), @ApiResponse(code = 401, message = "Unauthorized"),
			@ApiResponse(code = 403, message = "Forbidden"), @ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Internal Server Error") })
	public Response updateDataSourceKeyV2(@HeaderParam("Authorization") String authorization,
			@PathParam("datasetKey") String dataSetKey, @RequestBody DatasetDatasourceInfo dataSource);

	@GET
	@Path("/{datasetKey}/attributeMetaData")
	@Produces({ MediaType.APPLICATION_JSON })
	@ApiOperation(value = "Returns Attribute MetaData.", notes = "Returns a JSON Array of Attribute MetaData for the given Dataset.", response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "A successful response with a message body"),
			@ApiResponse(code = 400, message = "Bad Request"), @ApiResponse(code = 401, message = "Unauthorized"),
			@ApiResponse(code = 403, message = "Forbidden"), @ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Internal Server Error") })
	public Response getAttributeMetaDataV2(@HeaderParam("Authorization") String authorization,
			@PathParam("datasetKey") String dataSetKey);

	@PUT
	@Path("/{datasetKey}/attributeMetaData")
	@Produces({ MediaType.APPLICATION_JSON })
	@ApiOperation(value = "Respond contents of datasource associated with dataset.", notes = "Returns a character stream of data from the datsource associated with a dataset.", response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 200, message = "A successful response with a message body"),
			@ApiResponse(code = 400, message = "Bad Request"), @ApiResponse(code = 401, message = "Unauthorized"),
			@ApiResponse(code = 403, message = "Forbidden"), @ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Internal Server Error") })
	public Response updateAttributeMetaDataV2(@HeaderParam("Authorization") String authorization,
			@PathParam("datasetKey") String dataSetKey, @RequestBody DatasetAttributeMetaData attributeMetaData);
}
