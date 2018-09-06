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

import java.io.Serializable;

public class FileDetailsInfo implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	protected String fileURL;
	protected String fileFormat;
	protected String fileDelimitor;
	protected String fileServerUserName;
	protected String fileServerUserPassword;
	
	public String getFileURL() {
		return fileURL;
	}
	public void setFileURL(String fileURL) {
		this.fileURL = fileURL;
	}
	public String getFileFormat() {
		return fileFormat;
	}
	public void setFileFormat(String fileFormat) {
		this.fileFormat = fileFormat;
	}
	public String getFileDelimitor() {
		return fileDelimitor;
	}
	public void setFileDelimitor(String fileDelimitor) {
		this.fileDelimitor = fileDelimitor;
	}
	public String getFileServerUserName() {
		return fileServerUserName;
	}
	public void setFileServerUserName(String fileServerUserName) {
		this.fileServerUserName = fileServerUserName;
	}
	public String getFileServerUserPassword() {
		return fileServerUserPassword;
	}
	public void setFileServerUserPassword(String fileServerUserPassword) {
		this.fileServerUserPassword = fileServerUserPassword;
	}

}
