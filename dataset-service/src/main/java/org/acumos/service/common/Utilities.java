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

package org.acumos.service.common;

public class Utilities {
	
	
	/*
	private static Logger log = LoggerFactory.getLogger(Utilities.class);
	private static final String DATASOURCEURL = "datasource_ms_base_url_v2";
	private static final String DATASUFFIX = "datasource_details_fetch_suffix_v2";
	private static final String DATASETNAME = "datasetName";

	private Utilities() {
		super();
	}

	
	public static String getName(String user) {

		StringBuilder sb = new StringBuilder((user.indexOf('@') != -1) ? user.substring(0, user.indexOf('@')) : user);
		sb.append("_").append(String.valueOf(Instant.now().toEpochMilli())).append("_").append(getRandom());

		return sb.toString();
	}

	public static InputStream getDataSource(String authorization, String datasourceKey) throws CmlpDataSrcException {

		HttpGet request = null;
		InputStream inStream = null;

		try {
			String datasourceURL = HelperTool.getEnv(DATASOURCEURL,
					HelperTool.getComponentPropertyValue(DATASOURCEURL));
			String datasourceURI = HelperTool.getEnv(DATASUFFIX, HelperTool.getComponentPropertyValue(DATASUFFIX));

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
				CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum.E_0003, variables, null,
						CmlpApplicationEnum.DATASET);

				throw new CmlpDataSrcException(
						"Invalid data. Not a valid datasourceKey. Please send valid information.",
						Status.BAD_REQUEST.getStatusCode(), err);
			} else {
				// Read message from Datasource response and build new exception
				CmlpRestError err = getCmlpRestError(response, "Datasource @ " + request.getURI());

				throw new CmlpDataSrcException(
						"Oops.Something went wrong while contacting datasource manager. Datasource manager retuned: "
								+ response.getStatusLine().getReasonPhrase(),
						responseCode, err);
			}
		} catch (CmlpDataSrcException cmlpEx) {
			throw cmlpEx;

		} catch (Exception e) {
			CmlpRestError err = CmlpErrorList.buildError(e, null, CmlpApplicationEnum.DATASET);
			throw new CmlpDataSrcException("Exception occurred while calling Datasource Manager.",
					Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
		} finally {
			if (request != null) {
				request.releaseConnection();
			}
		}

		return inStream;
	}
	
	private static void addMissedFields (DatasetModel_Get dataset, List<String> missedParameters) {
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

	private static void findMissedParameters(DatasetModel_Get dataset) throws CmlpDataSrcException {
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

			CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum.E_0002, variables, null,
					CmlpApplicationEnum.DATASET);

			throw new CmlpDataSrcException(
					"DatasetModel has missing mandatory information. Please send all the required information.",
					Status.BAD_REQUEST.getStatusCode(), err);
		}
	}

	private static void validateDatasetType(DatasetModel_Get dataset) throws CmlpDataSrcException {
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

			CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum.E_0004, variables, null,
					CmlpApplicationEnum.DATASET);

			throw new CmlpDataSrcException(
					"DatasetModel has invalid DatasetType. Please send all the required information.",
					Status.BAD_REQUEST.getStatusCode(), err);

		}
	}

	public static void validateDatasetModel(DatasetModel_Get dataset, DbUtilities_v2 dbUtilities, String mode)
			throws CmlpDataSrcException, IOException, JSONException {

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

				CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum.E_0003, variables, null,
						CmlpApplicationEnum.DATASET);

				throw new CmlpDataSrcException(
						"DatasetModel has duplicate dataset name in the current namespace/environment.",
						Status.BAD_REQUEST.getStatusCode(), err);
			}
		} else if ("update".equals(mode)) {
			List<String> dbRecords = dbUtilities.isDuplicateRecordExists(dataset.getDatasetName(),
					dataset.getNamespace());

			for (String dbRecord : dbRecords) {
				String dbId = Utilities.getFieldValueFromJsonString(dbRecord, "_id");
				if (!dataset.getDatasetKey().equals(dbId)) {
					log.error("DatasetModel has duplicate datasetName/namespace/env. throwing excpetion..." + dbRecord);
					String[] variables = { DATASETNAME, "namespace" };

					CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum.E_0003, variables, null,
							CmlpApplicationEnum.DATASET);

					throw new CmlpDataSrcException(
							"DatasetModel has a duplicate record. Not a valid combination to change datasetName/namespace/environment.",
							Status.BAD_REQUEST.getStatusCode(), err);
				}
			}
		}

	}

	private static String getRandom() {
		return String.valueOf(RandomUtils.nextLong());
	}

	public static String getFieldValueFromJsonString(String jsonStr, String fieldName) throws JSONException {
		JSONObject jsonObj = new JSONObject(jsonStr);
		if (jsonObj.has(fieldName))
			return jsonObj.getString(fieldName);
		else
			return null;
	}

	public static int[] getRange(String inputStr) {
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

	public static boolean isHardDeleteTurnedOn() {
		boolean isHardDelete = false;
		try {
			String configValue = HelperTool.getEnv("dataset_hard_delete",
					HelperTool.getComponentPropertyValue("dataset_hard_delete"));
			if (configValue != null && configValue.equalsIgnoreCase("true"))
				isHardDelete = true;
		} catch (Exception e) {
			log.error("Error occurred while determining Hard Delete Turn ON property");
		}
		return isHardDelete;
	}

	public static boolean getBooleanFieldValueFromJsonString(String jsonStr, String fieldName) throws JSONException {
		JSONObject jsonObj = new JSONObject(jsonStr);
		boolean result = false;
		if (jsonObj.has(fieldName))
			return jsonObj.getBoolean(fieldName);
		return result;
	}

	public static String getDatasourceURI(String datasourceURI, String datasourceKey) {
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

	private static CmlpRestError getCmlpRestError(HttpResponse response, String backendNameURL) {
		try (BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))) {
			StringBuilder sb = new StringBuilder();
			String line = "";
			while ((line = rd.readLine()) != null) {
				sb.append(line);
				sb.append("\n");
			}

			if (sb.toString().trim().length() == 0)
				throw new CmlpDataSrcException("NULL response from the backend : " + backendNameURL,
						Status.INTERNAL_SERVER_ERROR.getStatusCode(), null);

			return CmlpErrorList.buildError(sb.toString(), backendNameURL);

		} catch (Exception e) {
			log.error("Issue in handling Http response from backend call. " + e.getMessage());
			return CmlpErrorList.buildError(e, null, CmlpApplicationEnum.DATASET);
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
			return null;
		}
	}

	public static String getDatasourceResponseFromInputStream(InputStream inStream) throws IOException {
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

	public static void executeWriteback(String authorization, String datasourceKey, FormDataBodyPart dataFile)
			throws CmlpDataSrcException, ParseException, IOException {

		HttpPost request = null;

		try {
			// request url
			String baseURL = HelperTool.getEnv(DATASOURCEURL, HelperTool.getComponentPropertyValue(DATASOURCEURL));
			StringBuilder urlBuffer = new StringBuilder();
			urlBuffer.append(baseURL).append("/").append(datasourceKey).append("/prediction");

			log.info("Calling Datasource with URL ------->: " + urlBuffer.toString());

			FileEntity entity = (FileEntity) dataFile.getEntity();

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
				CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum.E_0003, variables, null,
						CmlpApplicationEnum.DATASET);

				throw new CmlpDataSrcException("Failed to execute writeback on the datasource.",
						Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
			}
		} catch (CmlpDataSrcException cmlpEx) {
			throw cmlpEx;

		} catch (Exception e) {
			CmlpRestError err = CmlpErrorList.buildError(e, null, CmlpApplicationEnum.DATASET);
			throw new CmlpDataSrcException("Exception occurred while calling Datasource Manager.",
					Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
		} finally {
			if (request != null) {
				request.releaseConnection();
			}
		}
	}*/
}
