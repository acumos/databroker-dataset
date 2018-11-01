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

package org.acumos.dataset.utils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import javax.ws.rs.core.Response.Status;

import org.acumos.dataset.common.CmlpApplicationEnum;
import org.acumos.dataset.common.DataSetErrorList;
import org.acumos.dataset.common.DataSetRestError;
import org.acumos.dataset.common.ErrorListEnum;
import org.acumos.dataset.common.HelperTool;
import org.acumos.dataset.connection.DbUtilities_v2;
import org.acumos.dataset.enums.DataSetTypeEnum;
import org.acumos.dataset.exception.DataSetException;
import org.acumos.dataset.schema.DatasetModelGet;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.ParseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.mime.FormBodyPart;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ApplicationUtilities {
	private static Logger log = LoggerFactory.getLogger(ApplicationUtilities.class);
	private static final String DATASOURCEURL = "datasource_ms_base_url_v2";
	private static final String DATASUFFIX = "datasource_details_fetch_suffix_v2";
	private static final String DATASETNAME = "datasetName";

	@Autowired
	HelperTool helperTool;
	
	private ApplicationUtilities() {
		super();
	}

	
	public String getName(String user) {

		StringBuilder sb = new StringBuilder((user.indexOf('@') != -1) ? user.substring(0, user.indexOf('@')) : user);
		sb.append("_").append(String.valueOf(Instant.now().toEpochMilli())).append("_").append(getRandom());

		return sb.toString();
	}

	/**
	 * Execute request to get datasource details from datasource mS
	 */
	public InputStream getDataSource(String authorization, String datasourceKey) throws DataSetException {

		HttpGet request = null;
		InputStream inStream = null;

		try {
			String datasourceURL = helperTool.getEnv(DATASOURCEURL,
					helperTool.getComponentPropertyValue(DATASOURCEURL));
			String datasourceURI = helperTool.getEnv(DATASUFFIX, helperTool.getComponentPropertyValue(DATASUFFIX));

			datasourceURI = getDatasourceURI(datasourceURI, datasourceKey);

			HttpClient client = HttpClients.createDefault();

			// populating request to fetch details about a datasource
			request = new HttpGet(datasourceURL + datasourceURI);

			request.addHeader("Authorization", authorization);
			log.info("request url for getting the datasource details from Datasource Manager: " + request.getURI());

			HttpResponse response = client.execute(request);

			int responseCode = response.getStatusLine().getStatusCode();

			log.info("response for getting the datasource details from Datasource Manager: " + responseCode);

			if (responseCode == 200) {

				inStream = getInputStream(response);

			} else if (responseCode == 204 || responseCode == 400 || responseCode == 404) {
				String[] variables = { "datasourceKey" };
				DataSetRestError err = DataSetErrorList.buildError(ErrorListEnum.E_0003, variables, null,
						CmlpApplicationEnum.DATASET);

				throw new DataSetException(
						"Invalid data. Not a valid datasourceKey. Please send valid information.",
						Status.BAD_REQUEST.getStatusCode(), err);
			} else {
				// Read message from Datasource response and build new exception
				DataSetRestError err = getCmlpRestError(response, "Datasource @ " + request.getURI());

				throw new DataSetException(
						"Oops.Something went wrong while contacting datasource manager. Datasource manager retuned: "
								+ response.getStatusLine().getReasonPhrase(),
						responseCode, err);
			}
		} catch (DataSetException cmlpEx) {
			throw cmlpEx;

		} catch (Exception e) {
			DataSetRestError err = DataSetErrorList.buildError(e, null, CmlpApplicationEnum.DATASET);
			throw new DataSetException("Exception occurred while calling Datasource Manager.",
					Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
		} finally {
			if (request != null) {
				request.releaseConnection();
			}
		}

		return inStream;
	}
	
	private static void addMissedFields (DatasetModelGet dataset, List<String> missedParameters) {
		if (dataset.getDatasetMetaData().getDomainArea() == null)
			missedParameters.add("domainArea");
		if (dataset.getDatasetName() == null)
			missedParameters.add(DATASETNAME);
		if (dataset.getDatasetDescription() == null)
			missedParameters.add("datasetDescription");
		if (dataset.getDatasetMetaData().getDatasetType() == null)
			missedParameters.add("datasetType");
		if (dataset.getDatasetMetaData().getGeoStatisticalType() == null)
			missedParameters.add("datasetDataType");
		if (dataset.getDatasetMetaData().getTaskType() == null)
			missedParameters.add("taskType");
		if (dataset.getDatasetMetaData().getAttributeType() == null)
			missedParameters.add("attributeType");
		if (dataset.getDatasetMetaData().getFormatType() == null)
			missedParameters.add("formatType");
	}

	private static void findMissedParameters(DatasetModelGet dataset) throws DataSetException {
		if (dataset == null || dataset.getDatasetName() == null || dataset.getDatasetDescription() == null
				|| (dataset.getDatasetMetaData() != null && (dataset.getDatasetMetaData().getDomainArea() == null
						|| dataset.getDatasetMetaData().getDatasetType() == null
						|| dataset.getDatasetMetaData().getGeoStatisticalType() == null
						|| dataset.getDatasetMetaData().getTaskType() == null
						|| dataset.getDatasetMetaData().getAttributeType() == null
						|| dataset.getDatasetMetaData().getFormatType() == null))) {

			log.error("DatasetModel has missing mandatory information. throwing excpetion...");
			ArrayList<String> missedParameters = new ArrayList<>();
			if (null == dataset) {
				missedParameters.add("dataset");
			} else {
				addMissedFields(dataset, missedParameters);
			}

			String[] variables = new String[missedParameters.size()];

			for (int i = 0; i < missedParameters.size(); i++) {
				variables[i] = missedParameters.get(i);
			}

			DataSetRestError err = DataSetErrorList.buildError(ErrorListEnum.E_0002, variables, null,
					CmlpApplicationEnum.DATASET);

			throw new DataSetException(
					"DatasetModel has missing mandatory information. Please send all the required information.",
					Status.BAD_REQUEST.getStatusCode(), err);
		}
	}

	private static void validateDatasetType(DatasetModelGet dataset) throws DataSetException {
		if (!isValidDatasetType(dataset.getDatasetMetaData().getDatasetType())) {
			ArrayList<String> validValues = new ArrayList<>();

			validValues.add("datasetMetaData.datasetType");

			for (DataSetTypeEnum datasetType : DataSetTypeEnum.values()) {
				validValues.add(datasetType.getDatasetType());
			}

			String[] variables = new String[validValues.size()];

			for (int i = 0; i < validValues.size(); i++) {
				variables[i] = validValues.get(i);
			}

			DataSetRestError err = DataSetErrorList.buildError(ErrorListEnum.E_0004, variables, null,
					CmlpApplicationEnum.DATASET);

			throw new DataSetException(
					"DatasetModel has invalid DatasetType. Please send all the required information.",
					Status.BAD_REQUEST.getStatusCode(), err);

		}
	}

	public void validateDatasetModel(DatasetModelGet dataset, DbUtilities_v2 dbUtilities, String mode)
			throws DataSetException, IOException, JSONException {

		findMissedParameters(dataset);

		// check for certain field values
		// set datasetType as pure if no datasource associated
		if (dataset.getDatasourceKey() == null || dataset.getDatasourceKey().isEmpty())
			dataset.getDatasetMetaData().setDatasetType(DataSetTypeEnum.PURE.getDatasetType());
		else if (dataset.getDatasetMetaData().getDatasetType() == null
				|| dataset.getDatasetMetaData().getDatasetType().isEmpty())
			dataset.getDatasetMetaData().setDatasetType(DataSetTypeEnum.EVALUATE.getDatasetType());

		// Validate DatasetTypes
		validateDatasetType(dataset);

		if ("create".equals(mode)) {
			log.info("validate dataset name and namespace for duplicate...");

			if (!dbUtilities.isDuplicateRecordExists(dataset.getDatasetName(), dataset.getNamespace()).isEmpty()) {
				log.error("DatasetModel has duplicate dataset name in the same namespace. throwing excpetion...");
				String[] variables = { DATASETNAME, "namespace" };

				DataSetRestError err = DataSetErrorList.buildError(ErrorListEnum.E_0003, variables, null,
						CmlpApplicationEnum.DATASET);

				throw new DataSetException(
						"DatasetModel has duplicate dataset name in the current namespace/environment.",
						Status.BAD_REQUEST.getStatusCode(), err);
			}
		} else if ("update".equals(mode)) {
			List<String> dbRecords = dbUtilities.isDuplicateRecordExists(dataset.getDatasetName(),
					dataset.getNamespace());

			for (String dbRecord : dbRecords) {
				String dbId = getFieldValueFromJsonString(dbRecord, "_id");
				if (!dataset.getDatasetKey().equals(dbId)) {
					log.error("DatasetModel has duplicate datasetName/namespace/env. throwing excpetion..." + dbRecord);
					String[] variables = { DATASETNAME, "namespace" };

					DataSetRestError err = DataSetErrorList.buildError(ErrorListEnum.E_0003, variables, null,
							CmlpApplicationEnum.DATASET);

					throw new DataSetException(
							"DatasetModel has a duplicate record. Not a valid combination to change datasetName/namespace/environment.",
							Status.BAD_REQUEST.getStatusCode(), err);
				}
			}
		}

	}

	private static String getRandom() {
		return String.valueOf(RandomUtils.nextLong());
	}

	public String getFieldValueFromJsonString(String jsonStr, String fieldName) throws JSONException {
		JSONObject jsonObj = new JSONObject(jsonStr);
		if (jsonObj.has(fieldName))
			return jsonObj.getString(fieldName);
		else
			return null;
	}

	public int[] getRange(String inputStr) {
		int[] range = { 0, 0 };
		try {
			StringTokenizer st = new StringTokenizer(inputStr, "-"); // inputStr = 10-100
			range[0] = Integer.valueOf(st.nextToken().trim());
			range[1] = Integer.valueOf(st.nextToken().trim());
		} catch (Exception e) {
			log.error("Error occurred while determining the range for no.of Attributes/Instances.");
		}
		return range;
	}

	public boolean isHardDeleteTurnedOn() {
		boolean isHardDelete = false;
		try {
			String configValue = helperTool.getEnv("dataset_hard_delete",
					helperTool.getComponentPropertyValue("dataset_hard_delete"));
			if (configValue != null && configValue.equalsIgnoreCase("true"))
				isHardDelete = true;
		} catch (Exception e) {
			log.error("Error occurred while determining Hard Delete Turn ON property");
		}
		return isHardDelete;
	}

	public boolean getBooleanFieldValueFromJsonString(String jsonStr, String fieldName) throws JSONException {
		JSONObject jsonObj = new JSONObject(jsonStr);
		boolean result = false;
		if (jsonObj.has(fieldName))
			return jsonObj.getBoolean(fieldName);
		return result;
	}

	public String getDatasourceURI(String datasourceURI, String datasourceKey) {
		if (datasourceURI != null) {
			datasourceURI = datasourceURI.trim();

			if (datasourceURI.indexOf("{datasourceKey}") >= 0) {
				datasourceURI = datasourceURI.replace("{datasourceKey}", datasourceKey);

			} else {
				datasourceURI = datasourceURI + datasourceKey;
			}

			if (!datasourceURI.startsWith("?") && !datasourceURI.startsWith("/")) {
				datasourceURI = "/" + datasourceURI;
			}
		}

		return datasourceURI;
	}

	private static DataSetRestError getCmlpRestError(HttpResponse response, String backendNameURL) {
		try (BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))) {
			StringBuilder sb = new StringBuilder();
			String line = "";
			while ((line = rd.readLine()) != null) {
				sb.append(line);
				sb.append("\n");
			}

			if (sb.toString().trim().length() == 0)
				throw new DataSetException("NULL response from the backend : " + backendNameURL,
						Status.INTERNAL_SERVER_ERROR.getStatusCode(), null);

			return DataSetErrorList.buildError(sb.toString(), backendNameURL);

		} catch (Exception e) {
			log.error("Issue in handling Http response from backend call. " + e.getMessage());
			return DataSetErrorList.buildError(e, null, CmlpApplicationEnum.DATASET);
		}
	}

	private static InputStream getInputStream(HttpResponse response) {
		try (BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))) {
			StringBuilder result = new StringBuilder();
			String line = "";
			while ((line = rd.readLine()) != null) {
				result.append(line);
			}
			return (new ByteArrayInputStream(result.toString().getBytes()));
		} catch (Exception e) {
			log.error("Issue in handling Http response from HTTP response. " + e.getMessage());
			return null;
		}
	}

	public String getDatasourceResponseFromInputStream(InputStream inStream) throws IOException {
		BufferedReader rd = new BufferedReader(new InputStreamReader(inStream));
		String line = "";
		StringBuilder sb = new StringBuilder();
		while ((line = rd.readLine()) != null) {
			sb.append(line);
		}

		rd.close();

		return sb.toString();
	}

	private static boolean isValidDatasetType(String type) {
		boolean isValid = false;

		for (DataSetTypeEnum datasetType : DataSetTypeEnum.values()) {
			if (datasetType.getDatasetType().equalsIgnoreCase(type)) {
				isValid = true;
				break;
			}
		}

		return isValid;
	}

	public void executeWriteback(String authorization, String datasourceKey, FormBodyPart dataFile)
			throws DataSetException, ParseException, IOException {

		HttpPost request = null;

		try {
			
			String baseURL = helperTool.getEnv(DATASOURCEURL, helperTool.getComponentPropertyValue(DATASOURCEURL));
			StringBuilder urlBuffer = new StringBuilder();
			urlBuffer.append(baseURL).append("/").append(datasourceKey).append("/prediction");

			log.info("Calling Datasource with URL ------->: " + urlBuffer.toString());

			FileEntity entity = (FileEntity) dataFile.getBody();

			HttpClient client = HttpClients.createDefault();

			// populating request to fetch details about a datasource
			request = new HttpPost(urlBuffer.toString());
			request.addHeader("Authorization", authorization);
			request.setEntity(entity);

			HttpResponse response = client.execute(request);

			log.info("Datasource execute prediction - StatusLine: " + response.getStatusLine());
			log.info("-------> response status: " + response.getStatusLine().getStatusCode());

			// response status check
			if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
				log.error("Failed to execute writeback on the datasource.");
				String[] variables = { "dataFile" };
				DataSetRestError err = DataSetErrorList.buildError(ErrorListEnum.E_0003, variables, null,
						CmlpApplicationEnum.DATASET);

				throw new DataSetException("Failed to execute writeback on the datasource.",
						Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
			}
		} catch (DataSetException cmlpEx) {
			throw cmlpEx;

		} catch (Exception e) {
			DataSetRestError err = DataSetErrorList.buildError(e, null, CmlpApplicationEnum.DATASET);
			throw new DataSetException("Exception occurred while calling Datasource Manager.",
					Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
		} finally {
			if (request != null) {
				request.releaseConnection();
			}
		}
	}
}
