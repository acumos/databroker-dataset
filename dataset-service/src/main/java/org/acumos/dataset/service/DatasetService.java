/*-
 * ===============LICENSE_START=======================================================
 * Acumos
 * ===================================================================================
 * Copyright (C) 2017 AT&T Intellectual Property. All rights reserved.
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

package org.acumos.dataset.service;

import java.net.URI;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.acumos.dataset.common.CmlpApplicationEnum;
import org.acumos.dataset.common.CmlpErrorList;
import org.acumos.dataset.common.CmlpRestError;
import org.acumos.dataset.common.ErrorListEnum;
import org.acumos.dataset.common.FilterResults;
import org.acumos.dataset.common.HelperTool;
import org.acumos.dataset.exception.DataSetException;
import org.acumos.dataset.schema.DataSetSearchKeys;
import org.acumos.dataset.schema.DatasetAttributeMetaData;
import org.acumos.dataset.schema.DatasetDatasourceInfo;
import org.acumos.dataset.schema.DatasetModelPost;
import org.acumos.dataset.schema.DatasetModelPut;
import org.acumos.dataset.utils.ApplicationUtilities;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;


//@Service
public class DatasetService {

	private static Logger log = LoggerFactory.getLogger(DatasetService.class);
	private static final String DATASETKEY = "datasetKey";
	private static final String DATASETNOTFOUNDMSG = "No Dataset information available for the given datasetKey/Search.";
	private static final String DATASOURCESAMPLECONTENT = "datasource_details_sample_contents";
	
	@Autowired
	HttpServletRequest request;
	
	@Autowired
	DataSetServiceV2 service;

	
	public Response getDataSetListV2(String authorization, String textSearch, DataSetSearchKeys dataSearchKeys,int pgOffset, int pgLimit) {

		String remoteUser;
		List<String> results = null;

		try {
			// check for input values
			if ((pgOffset > 0 && pgLimit == 0) || (pgOffset == 0 && pgLimit > 0)) {
				String[] variables = { "page", "perPage" };
				CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum.E_0003, variables, null,
						CmlpApplicationEnum.DATASET);

				throw new DataSetException("Invalid input: Both Offset and Limit values should be either 0s or > 0",
						Status.NOT_FOUND.getStatusCode(), err);
			}

			String url = HelperTool.getResourceURL(request);

			remoteUser = HelperTool.getRemoteUser(request);
			log.info("getdatasetsList with user boolean as true, remote user detail: " + remoteUser);

			String datasetKey = null;

			if (textSearch == null && dataSearchKeys == null)
				datasetKey = "ALL";

			results = service.getDataSetDetails(remoteUser, datasetKey, textSearch, dataSearchKeys);

			log.info("getdatasetsList(), Total No. of datasets are: " + results.size());

			log.debug(results.toString());

			int totalRecords = results.size();

			if (results != null && !results.isEmpty()) {

				if (pgOffset == 0 && pgLimit == 0) { // set default
					pgOffset = 1;
					pgLimit = 25;
				}

				results = FilterResults.returnDatasets(results, pgOffset, pgLimit);

				if (pgOffset > 0 && pgLimit > 0) {

					if ((!results.isEmpty() && (pgOffset * pgLimit) < totalRecords)
							|| (!results.isEmpty() && (pgOffset * pgLimit) >= totalRecords)) {
						log.info("getdatasetsList(), No. of datasets returning are: " + results.size());

						ResponseBuilder responseBuilder = Response.ok(results.toString());
						return FilterResults.setPaginationRecord(pgOffset, pgLimit, totalRecords, url, responseBuilder);

					} else {

						log.info(
								"getdatasetsList() requested range exceeds total number of datasets. Returning zero datasets.");
						StringBuilder sb = new StringBuilder();
						sb.append(url).append("?page=").append(1).append("&perPage=").append(pgLimit);

						return Response.status(Status.OK.getStatusCode()).link(sb.toString(), "first").build();
					}
				} else {

					return Response.ok(results.toString()).build();
				}
			} else if (pgOffset > 0 && pgLimit > 0) { // zero records for the given search, with pagination

				log.info(
						"getdatasetsList() requested range exceeds total number of datasets. Returning zero datasets.");

				StringBuilder sb = new StringBuilder();
				sb.append(url).append("?page=").append(1).append("&perPage=").append(pgLimit);

				return Response.status(Status.OK.getStatusCode()).link(sb.toString(), "first").build();

			} else { // zero records for the given search

				return Response.ok("").build();
			}

		} catch (DataSetException cmlpException) {

			return cmlpException.toResponse();

		} catch (Exception e) {

			CmlpRestError err = CmlpErrorList.buildError(e, null, CmlpApplicationEnum.DATASET);

			return Response.status(Status.INTERNAL_SERVER_ERROR.getStatusCode()).entity(err).build();
		}
	}

	public Response getDataSetV2(String authorization, String datasetKey) {

		String remoteUser;
		List<String> results = null;
		try {
			log.info("getdatasetsList, datasetKey value being passed: " + datasetKey);

			remoteUser = HelperTool.getRemoteUser(request);
			log.info("getdatasetsList with user boolean as true, remote user detail: " + remoteUser);

			results = service.getDataSetDetails(remoteUser, datasetKey, null, null);
			log.info("getdatasetsList with user boolean as true, No. of datasets returning are: " + results.size());
			log.debug(results.toString());

			if (!results.isEmpty()) {
				return Response.ok(results.get(0)).build();
			} else {

				String[] variables = new String[1];

				if (datasetKey != null) {
					variables[0] = DATASETKEY;
				}
				CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum.E_0003, variables, null,
						CmlpApplicationEnum.DATASET);

				throw new DataSetException(DATASETNOTFOUNDMSG, Status.NOT_FOUND.getStatusCode(), err);
			}

		} catch (DataSetException cmlpException) {
			return cmlpException.toResponse();

		} catch (Exception e) {

			CmlpRestError err = CmlpErrorList.buildError(e, null, CmlpApplicationEnum.DATASET);

			return Response.status(Status.INTERNAL_SERVER_ERROR.getStatusCode()).entity(err).build();
		}
	}

	public Response saveDataSetDetailV2(String authorization, DatasetModelPost dataSet) {
		String remoteUser;
		try {
			remoteUser = HelperTool.getRemoteUser(request);

			String url = HelperTool.getResourceURL(request);

			String apiVersion = HelperTool.getAPIVersion(request); // initiating API version

			log.info("savedatasetDetail_v2, Request version : " + apiVersion);

			log.info("savedatasetDetail_v2, remote user detail: " + remoteUser);
			String datasetKey = service.insertDataSet(authorization, remoteUser, dataSet);

			log.info("savedatasetDetail_v2, datasetmodel has been saved. Returning OK response...");
			JSONObject json = new JSONObject();

			json.put(DATASETKEY, datasetKey);
			json.put("resourceURL", url + "/" + datasetKey);

			URI location = new URI(url + "/" + datasetKey);

			return Response.created(location).entity(json.toString()).build();

		} catch (DataSetException cmlpException) {
			return cmlpException.toResponse();
		} catch (Exception e) {

			CmlpRestError err = CmlpErrorList.buildError(e, null, CmlpApplicationEnum.DATASET);

			return Response.status(Status.INTERNAL_SERVER_ERROR.getStatusCode()).entity(err).build();
		}
	}

	public Response updateDataSetDetailV2(String authorization, String datasetKey, DatasetModelPut dataset) {
		String remoteUser;
		try {
			remoteUser = HelperTool.getRemoteUser(request);
			log.info("updatedatasetDetail, remote user detail: " + remoteUser);

			if (dataset.getDatasourceKey() != null) {
				StringBuilder datasourceURL = new StringBuilder(); // init it
				datasourceURL.append(HelperTool.getEnv("datasource_ms_base_url",
						HelperTool.getComponentPropertyValue("datasource_ms_base_url")));
				StringBuilder datasourceSampleURL = new StringBuilder();
				datasourceSampleURL.append("/datasourceKey/" + HelperTool.getEnv(DATASOURCESAMPLECONTENT,
						HelperTool.getComponentPropertyValue(DATASOURCESAMPLECONTENT)));
				StringBuilder datasourceContentsUrl = new StringBuilder();
				datasourceContentsUrl.append(HelperTool.getEnv(DATASOURCESAMPLECONTENT,
						HelperTool.getComponentPropertyValue(DATASOURCESAMPLECONTENT)) + "/datasourceKey");
				datasourceURL.append(request.getRequestURI());

			}

			boolean update = service.updateDataSetDetail(authorization, remoteUser, datasetKey, dataset);
			log.info("updatedatasetDetail, the details for " + datasetKey
					+ " was attempeted for update and it resulted in boolean flag : " + update);

			return getDataSetV2(authorization, datasetKey);

		} catch (DataSetException cmlpException) {
			return cmlpException.toResponse();
		} catch (Exception e) {

			CmlpRestError err = CmlpErrorList.buildError(e, null, CmlpApplicationEnum.DATASET);

			return Response.status(Status.INTERNAL_SERVER_ERROR.getStatusCode()).entity(err).build();
		}
	}

	public Response deleteDataSetDetailV2(String authorization, String datasetKey) {
		String remoteUser;
		try {
			remoteUser = HelperTool.getRemoteUser(request);
			log.info("deletedatasetDetail, remote user detail: " + remoteUser);
			boolean isDeleted = service.deleteDataSetDetail(remoteUser, datasetKey);
			log.info("deletedatasetDetail, the details for " + datasetKey
					+ " was attempeted for delete and it resulted in boolean flag : " + isDeleted);

			if (isDeleted) {
				JSONObject json = new JSONObject();

				if (ApplicationUtilities.isHardDeleteTurnedOn())
					json.put("status", "Success - Dataset has been permanently deleted.");
				else
					json.put("status", "Success - Dataset has been successfully deleted.");

				return Response.status(Status.NO_CONTENT.getStatusCode()).entity(json.toString()).build();
			} else {
				throw new DataSetException("Unknown Exception occuured while deleting the Dataset.",
						Status.INTERNAL_SERVER_ERROR.getStatusCode(), null);
			}
		} catch (DataSetException cmlpException) {
			return cmlpException.toResponse();

		} catch (Exception e) {
			log.info("deleteDataSetDetail, Unknown Exception : " + e.getMessage());
			CmlpRestError err = CmlpErrorList.buildError(e, null, CmlpApplicationEnum.DATASET);
			return Response.status(Status.INTERNAL_SERVER_ERROR.getStatusCode()).entity(err).build();
		}
	}

	public Response getDataSourcesV2(String authorization, String dataSetKey) {
		String remoteUser;
		try {
			remoteUser = HelperTool.getRemoteUser(request);
			log.info("getDataSources, remote user detail: " + remoteUser);
			String datasourceKey = service.getDataSources(authorization, remoteUser, dataSetKey);
			log.info("getDataSources, the data source for " + dataSetKey + " are returning... ");

			JSONObject result = new JSONObject();
			result.put("datasourceKey", datasourceKey);

			return Response.status(Status.OK.getStatusCode()).entity(result.toString()).build();

		} catch (DataSetException cmlpException) {
			return cmlpException.toResponse();
		} catch (Exception e) {

			CmlpRestError err = CmlpErrorList.buildError(e, null, CmlpApplicationEnum.DATASET);

			return Response.status(Status.INTERNAL_SERVER_ERROR.getStatusCode()).entity(err).build();
		}
	}

	public Response updateDataSourceKeyV2(String authorization, String dataSetKey, DatasetDatasourceInfo dataSource) {
		String remoteUser;
		try {
			remoteUser = HelperTool.getRemoteUser(request);
			log.info("updateDataSourceKey, remote user detail: " + remoteUser);
			boolean update = service.updateDataSourceKey(authorization, remoteUser, dataSetKey,
					dataSource.getDatasourceKey());

			if (update) {
				return getDataSourcesV2(authorization, dataSetKey);

			} else {
				String[] variables = new String[1];

				if (dataSetKey != null) {
					variables[0] = DATASETKEY;
				}

				CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum.E_0003, variables, null,
						CmlpApplicationEnum.DATASET);

				throw new DataSetException(DATASETNOTFOUNDMSG, Status.NOT_FOUND.getStatusCode(), err);
			}
		} catch (DataSetException cmlpException) {
			return cmlpException.toResponse();
		} catch (Exception e) {

			CmlpRestError err = CmlpErrorList.buildError(e, null, CmlpApplicationEnum.DATASET);

			return Response.status(Status.INTERNAL_SERVER_ERROR.getStatusCode()).entity(err).build();
		}
	}

	public Response getAttributeMetaDataV2(String authorization, String datasetKey) {

		String remoteUser;
		List<String> results = null;
		try {
			log.info("getAttributeMetaData, datasetKey value being passed: " + datasetKey);

			remoteUser = HelperTool.getRemoteUser(request);
			log.info("getAttributeMetaData with user boolean as true, remote user detail: " + remoteUser);

			results = service.getDataSetDetails(remoteUser, datasetKey, null, null);
			log.info(
					"getAttributeMetaData with user boolean as true, No. of datasets returning are: " + results.size());
			log.debug(results.toString());

			if (!results.isEmpty()) {
				JSONObject jsonObj = new JSONObject(results.get(0));

				if (jsonObj.has("attributeMetaData")) {
					JSONObject result = jsonObj.getJSONObject("attributeMetaData");

					return Response.ok(result.toString()).build();

				} else {
					String[] variables = new String[1];

					if (datasetKey != null) {
						variables[0] = DATASETKEY;
					}

					CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum.E_0003, variables, null,
							CmlpApplicationEnum.DATASET);

					throw new DataSetException(
							"No Attribute MetaData information available for the given datasetKey/Search.",
							Status.NO_CONTENT.getStatusCode(), err);
				}
			}

			String[] variables = new String[1];

			if (datasetKey != null) {
				variables[0] = DATASETKEY;
			}

			CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum.E_0003, variables, null,
					CmlpApplicationEnum.DATASET);

			throw new DataSetException(DATASETNOTFOUNDMSG, Status.NOT_FOUND.getStatusCode(), err);

		} catch (DataSetException cmlpException) {
			return cmlpException.toResponse();

		} catch (Exception e) {

			CmlpRestError err = CmlpErrorList.buildError(e, null, CmlpApplicationEnum.DATASET);

			return Response.status(Status.INTERNAL_SERVER_ERROR.getStatusCode()).entity(err).build();
		}
	}

	public Response updateAttributeMetaDataV2(String authorization, String datasetKey,DatasetAttributeMetaData attributeMetaData) {
		String remoteUser;
		try {
			remoteUser = HelperTool.getRemoteUser(request);
			log.info("updateAttributeMetaData, remote user detail: " + remoteUser);

			boolean update = service.updateAttributeMetaData(authorization, remoteUser, datasetKey, attributeMetaData);

			log.info("updateAttributeMetaData, the details for " + datasetKey
					+ " was attempeted for update and it resulted in boolean flag : " + update);
			if (update) {

				return getAttributeMetaDataV2(authorization, datasetKey);

			} else {
				String[] variables = new String[1];

				if (datasetKey != null) {
					variables[0] = DATASETKEY;
				}

				CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum.E_0003, variables, null,
						CmlpApplicationEnum.DATASET);

				throw new DataSetException(DATASETNOTFOUNDMSG, Status.NOT_FOUND.getStatusCode(), err);
			}

		} catch (DataSetException cmlpException) {
			return cmlpException.toResponse();
		} catch (Exception e) {

			CmlpRestError err = CmlpErrorList.buildError(e, null, CmlpApplicationEnum.DATASET);

			return Response.status(Status.INTERNAL_SERVER_ERROR.getStatusCode()).entity(err).build();
		}
	}
}
