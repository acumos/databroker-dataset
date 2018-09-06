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

package org.acumos.datasource.model;

import org.acumos.datasource.schema.DataSourceMetadata;

public class FileConnectionModel {
		
		private String fileURL; //fileURL
		private String username;//dbServerUsername
		private String password;//dbServerPassword
		private DataSourceMetadata metaData; 
		
		public DataSourceMetadata getMetaData() {
			return metaData;
		}
		public void setMetaData(DataSourceMetadata metaData) {
			this.metaData = metaData;
		}
		public String getFileURL() {
			return fileURL;
		}
		public void setFileURL(String url) {
			this.fileURL = url;
		}
		public String getUsername() {
			return username;
		}
		public void setUsername(String username) {
			this.username = username;
		}
		public String getPassword() {
			return password;
		}
		public void setPassword(String password) {
			this.password = password;
		}
}
