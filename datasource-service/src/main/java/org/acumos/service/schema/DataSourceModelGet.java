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

package org.acumos.service.schema;

public class DataSourceModelGet extends DataSourceModelPut {
	
	private static final long serialVersionUID = 1L;

	protected String datasourceKey;
	protected String ownedBy;
	protected String createdTimestamp;
	protected String updatedTimestamp;
	protected DataSourceMetadata metaData;
	
	public DataSourceModelGet() {
		super();
	}
	
	public DataSourceModelGet(DataSourceModelPost post) {
		this.category = post.category;
		if (post.namespace != null)
			this.namespace = post.namespace.toLowerCase();
		this.datasourceName = post.datasourceName;
		this.datasourceDescription = post.datasourceDescription;
		this.readWriteDescriptor = post.readWriteDescriptor;
		this.predictorKey = post.predictorKey;
		this.commonDetails = post.commonDetails;
		this.fileDetails = post.fileDetails;
		this.dbDetails = post.dbDetails;
		this.hdfsHiveDetails = post.hdfsHiveDetails;
		this.serviceMetaData = post.serviceMetaData;
		this.customMetaData = post.customMetaData;
	}
	
	public DataSourceModelGet(DataSourceModelPut put) {
		this.category = put.category;
		if (put.namespace != null)
			this.namespace = put.namespace.toLowerCase();
		this.datasourceName = put.datasourceName;
		this.datasourceDescription = put.datasourceDescription;
		this.readWriteDescriptor = put.readWriteDescriptor;
		this.predictorKey = put.predictorKey;
		this.commonDetails = put.commonDetails;
		this.fileDetails = put.fileDetails;
		this.dbDetails = put.dbDetails;
		this.hdfsHiveDetails = put.hdfsHiveDetails;
		this.serviceMetaData = put.serviceMetaData;
		this.customMetaData = put.customMetaData;
		this.version = put.version;
		this.isActive = put.isActive;
	}
	
	public String getDatasourceKey() {
		return datasourceKey;
	}
	public void setDatasourceKey(String datasourceKey) {
		this.datasourceKey = datasourceKey;
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

	public DataSourceMetadata getMetaData() {
		return metaData;
	}
	public void setMetaData(DataSourceMetadata metaData) {
		this.metaData = metaData;
	}
}
