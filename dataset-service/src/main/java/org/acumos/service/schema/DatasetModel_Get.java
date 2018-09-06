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

import com.fasterxml.jackson.annotation.JsonProperty;

public class DatasetModel_Get extends DatasetModel_Put {
	
	protected String datasetKey;
	protected String ownedBy;
	protected String createdTimestamp;
	protected String updatedTimestamp;
	
	public DatasetModel_Get() {
		super();
	}
	
	public DatasetModel_Get(DatasetModel_Post post) {
		this.namespace = post.namespace;
		this.datasetName = post.datasetName;
		this.datasetDescription = post.datasetDescription;
		this.datasourceKey = post.datasourceKey;
		this.isHeaderRowAvailable = post.isHeaderRowAvailable;
		this.datasetMetaData = post.datasetMetaData;
		this.attributeMetaData = post.attributeMetaData;
		this.serviceMetaData = post.serviceMetaData;
		this.customMetaData = post.customMetaData;
	}
	
	public DatasetModel_Get(DatasetModel_Put put) {
		this.namespace = put.namespace;
		this.datasetName = put.datasetName;
		this.datasetDescription = put.datasetDescription;
		this.datasourceKey = put.datasourceKey;
		this.isHeaderRowAvailable = put.isHeaderRowAvailable;
		this.datasetMetaData = put.datasetMetaData;
		this.attributeMetaData = put.attributeMetaData;
		this.serviceMetaData = put.serviceMetaData;
		this.customMetaData = put.customMetaData;
		this.version = put.version;
	}
	
	//R1.6
	protected boolean isActive;
	
	public String getDatasetKey() {
		return datasetKey;
	}

	public void setDatasetKey(String datasetKey) {
		this.datasetKey = datasetKey;
	}

	public String getOwnedBy() {
		return ownedBy;
	}

	public void setOwnedBy(String ownedBy) {
		this.ownedBy = ownedBy;
	}

	public String getCreatedTimestamp() {
		return createdTimestamp;
	}

	public void setCreatedTimestamp(String createdTimestamp) {
		this.createdTimestamp = createdTimestamp;
	}

	public String getUpdatedTimestamp() {
		return updatedTimestamp;
	}

	public void setUpdatedTimestamp(String updatedTimestamp) {
		this.updatedTimestamp = updatedTimestamp;
	}

	@JsonProperty("isActive")
	public boolean isActive() {
		return isActive;
	}

	@JsonProperty("isActive")
	public void setActive(boolean isActive) {
		this.isActive = isActive;
	} 
}
