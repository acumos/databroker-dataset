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

package org.acumos.service.schema;

public class DataSetSearchKeys {

	/*
	private static final String FORMAT_TYPE = "formatType";

	private static final String DOMAIN_AREA = "domainArea";

	private static final String DATASOURCE_NAME = "datasourceName";

	private static final String DATASET_TYPE = "datasetType";

	private static final String DATASET_NAME = "datasetName";

	private static final String DATASET_DESCRIPTION = "datasetDescription";

	private static final String DATASET_DATA_TYPE = "datasetDataType";

	private static final String ATTRIBUTE_TYPE = "attributeType";

	private static final String NAMESPACE2 = "namespace";

	private static final String DATASET_KEY = "datasetKey";

	private static final String INVALID_SEARCH_PARAMETERS_PLEASE_TRY_NEW_SEARCH = "Invalid search parameters. Please try new search.";


	private String datasetKey;
	private String namespace;
	private String attributeType; // Categorical, Numerical, Mixed
	private String datasetDataType; // Multivariate, Univariate, Sequential, Time Series, Text, Domain-Theory, Other
	private String datasetDescription;
	private String datasetName;
	private String datasetType; // Training, Testing, Validation, Evaluation
	private String datasourceName;
	private String domainArea; // Domain1, Domain2, etc
	private String formatType; // Matrix, Non-Matrix
	
	private List<String> noOfAttributes; // 10-100, 100-200 -> Columns
	private List<String> noOfInstances; // 0-100, 100-1000, 1000-10000 -> Rows
	private String taskType; // Classification, Regression, Clustering, Other
	private List<DataMetaDataInfo> dataMetaDataInfo;
	private List<DataSetKVPair> keyValuePairs;

	// Deserializes an Object of class MyClass from its JSON representation
	public static DataSetSearchKeys fromString(String json)  {
		ObjectMapper mapper = new ObjectMapper(); // Jackson's JSON marshaller
		DataSetSearchKeys keys = null;
		try {
			keys = mapper.readValue(json, DataSetSearchKeys.class);
		} catch (IOException e) {
			throw new WebApplicationException(INVALID_SEARCH_PARAMETERS_PLEASE_TRY_NEW_SEARCH,
					Status.BAD_REQUEST.getStatusCode());
		}
		return keys;
	}

	@Override
	public String toString() {
		JSONObject jsonObj = new JSONObject();
		try {
			jsonObj.put(DATASET_KEY, this.datasetKey);
			jsonObj.put(NAMESPACE2, this.namespace);
			jsonObj.put(ATTRIBUTE_TYPE, this.attributeType);
			jsonObj.put(DATASET_DATA_TYPE, this.datasetDataType);
			jsonObj.put(DATASET_DESCRIPTION, this.datasetDescription);
			jsonObj.put(DATASET_NAME, this.datasetName);
			jsonObj.put(DATASET_TYPE, this.datasetType);
			jsonObj.put(DATASOURCE_NAME, this.datasourceName);
			jsonObj.put(DOMAIN_AREA, this.domainArea);
			jsonObj.put(FORMAT_TYPE, this.formatType);
		} catch (JSONException e) {
			e.printStackTrace();
		}

		return jsonObj.toString();
	}

	public String getDatasetKey() {
		return datasetKey;
	}

	public void setDatasetKey(String datasetKey) {
		this.datasetKey = datasetKey;
	}

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public String getAttributeType() {
		return attributeType;
	}

	public void setAttributeType(String attributeType) {
		this.attributeType = attributeType;
	}

	public String getDatasetDataType() {
		return datasetDataType;
	}

	public void setDatasetDataType(String datasetDataType) {
		this.datasetDataType = datasetDataType;
	}

	public String getDatasetDescription() {
		return datasetDescription;
	}

	public void setDatasetDescription(String datasetDescription) {
		this.datasetDescription = datasetDescription;
	}

	public String getDatasetName() {
		return datasetName;
	}

	public void setDatasetName(String datasetName) {
		this.datasetName = datasetName;
	}

	public String getDatasetType() {
		return datasetType;
	}

	public void setDatasetType(String datasetType) {
		this.datasetType = datasetType;
	}

	public String getDatasourceName() {
		return datasourceName;
	}

	public void setDatasourceName(String datasourceName) {
		this.datasourceName = datasourceName;
	}

	public String getDomainArea() {
		return domainArea;
	}

	public void setDomainArea(String domainArea) {
		this.domainArea = domainArea;
	}

	public String getFormatType() {
		return formatType;
	}

	public void setFormatType(String formatType) {
		this.formatType = formatType;
	}

	public List<String> getNoOfAttributes() {
		return noOfAttributes;
	}

	public void setNoOfAttributes(List<String> noOfAttributes) {
		this.noOfAttributes = noOfAttributes;
	}

	public List<String> getNoOfInstances() {
		return noOfInstances;
	}

	public void setNoOfInstances(List<String> noOfInstances) {
		this.noOfInstances = noOfInstances;
	}

	public String getTaskType() {
		return taskType;
	}

	public void setTaskType(String taskType) {
		this.taskType = taskType;
	}

	public List<DataMetaDataInfo> getDataMetaDataInfo() {
		return dataMetaDataInfo;
	}

	public void setDataMetaDataInfo(List<DataMetaDataInfo> dataMetaDataInfo) {
		this.dataMetaDataInfo = dataMetaDataInfo;
	}

	public List<DataSetKVPair> getKeyValuePairs() {
		return keyValuePairs;
	}

	public void setKeyValuePairs(List<DataSetKVPair> keyValuePairs) {
		this.keyValuePairs = keyValuePairs;
	}
	*/
	
	
}
