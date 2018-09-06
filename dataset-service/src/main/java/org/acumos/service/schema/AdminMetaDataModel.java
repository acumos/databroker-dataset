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

public class AdminMetaDataModel {
	private String adminMetadataKey;
	private String namespace;
	private String user;
	private String adminMetadata; //Metadata type
	private String metadata;
	private String createTimestamp;
	private String updateTimestamp;
	private String activeIndicator;
	
	
	public String getNamespace() {
		return namespace;
	}
	
	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}
	
	public String getUser() {
		return user;
	}
	
	public void setUser(String user) {
		this.user = user;
	}
	
	public String getCreateTimestamp() {
		return createTimestamp;
	}
	
	public void setCreateTimestamp(String createTimestamp) {
		this.createTimestamp = createTimestamp;
	}
	
	public String getUpdateTimestamp() {
		return updateTimestamp;
	}
	
	public void setUpdateTimestamp(String updateTimestamp) {
		this.updateTimestamp = updateTimestamp;
	}
	
	public String getActiveIndicator() {
		return activeIndicator;
	}
	
	public void setActiveIndicator(String activeIndicator) {
		this.activeIndicator = activeIndicator;
	}
	
	public String getAdminMetadata() {
		return adminMetadata;
	}
	
	public void setAdminMetadata(String adminMetadata) {
		this.adminMetadata = adminMetadata;
	}
	
	public String getAdminMetadataKey() {
		return adminMetadataKey;
	}
	
	public void setAdminMetadataKey(String adminMetadataKey) {
		this.adminMetadataKey = adminMetadataKey;
	}
	
	public String getMetadata() {
		return metadata;
	}
	
	public void setMetadata(String metadata) {
		this.metadata = metadata;
	}
}
