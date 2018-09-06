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

package org.acumos.dataset.schema;

import java.util.ArrayList;
import java.util.List;

import org.acumos.dataset.schema.DataSetKVPair;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DatasetModelPost {
	protected String namespace;
	protected String datasetName;
	protected String datasetDescription;
	protected String datasourceKey;
	protected boolean isHeaderRowAvailable;
	protected DatasetAttributeInfo datasetMetaData;
	protected int noOfAttributes = 0;
	protected DatasetAttributeMetaData attributeMetaData;
	protected List<DataSetKVPair> serviceMetaData;
	protected List<DataSetKVPair> customMetaData;
	
	
	public DatasetModelPost() {
		super();
		this.datasetMetaData = new DatasetAttributeInfo();
		this.attributeMetaData = new DatasetAttributeMetaData();
		this.serviceMetaData = new ArrayList<>();
		this.customMetaData = new ArrayList<>();
		
		init();
	}
	
	public void init() {
		this.serviceMetaData.add(new DataSetKVPair());
		this.customMetaData.add(new DataSetKVPair());
	}
	
	public String getNamespace() {
		return namespace;
	}
	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}
	public String getDatasetName() {
		return datasetName;
	}
	public void setDatasetName(String datasetName) {
		this.datasetName = datasetName;
	}
	public String getDatasetDescription() {
		return datasetDescription;
	}
	public void setDatasetDescription(String datasetDescription) {
		this.datasetDescription = datasetDescription;
	}
	public String getDatasourceKey() {
		return datasourceKey;
	}
	public void setDatasourceKey(String datasourceKey) {
		this.datasourceKey = datasourceKey;
	}
	
	@JsonProperty("isHeaderRowAvailable")
	public boolean isHeaderRowAvailable() {
		return isHeaderRowAvailable;
	}
	
	@JsonProperty("isHeaderRowAvailable")
	public void setHeaderRowAvailable(boolean isHeaderRowAvailable) {
		this.isHeaderRowAvailable = isHeaderRowAvailable;
	}

	public int getNoOfAttributes() {
		return noOfAttributes;
	}
	public void setNoOfAttributes(int noOfAttributes) {
		this.noOfAttributes = noOfAttributes;
	}
	
	public List<DataSetKVPair> getServiceMetaData() {
		return serviceMetaData;
	}

	public void setServiceMetaData(List<DataSetKVPair> serviceMetaData) {
		this.serviceMetaData = serviceMetaData;
	}

	public List<DataSetKVPair> getCustomMetaData() {
		return customMetaData;
	}
	public void setCustomMetaData(List<DataSetKVPair> customMetaData) {
		this.customMetaData = customMetaData;
	}

	public DatasetAttributeInfo getDatasetMetaData() {
		return datasetMetaData;
	}

	public void setDatasetMetaData(DatasetAttributeInfo datasetMetaData) {
		this.datasetMetaData = datasetMetaData;
	}

	public DatasetAttributeMetaData getAttributeMetaData() {
		return attributeMetaData;
	}

	public void setAttributeMetaData(DatasetAttributeMetaData attributeMetaData) {
		this.attributeMetaData = attributeMetaData;
	}	
}
