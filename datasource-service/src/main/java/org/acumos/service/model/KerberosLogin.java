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

package org.acumos.service.model;

import org.acumos.service.schema.DataSourceMetadata;

public class KerberosLogin {
	String kerberosLoginUser;
	String kerberosKeyTabContent;
	String kerberosRealms;
	String kerberosKdc;
	String kerbersoAdminServer;
	String kerberosPasswordServer;
	String kerberosDomainName;
	private DataSourceMetadata metaData; 
	String kerberosKeyTabFileName;
	String kerberosConfigFileName;
	
	
	
	public String getKerberosConfigFileName() {
		return kerberosConfigFileName;
	}

	public void setKerberosConfigFileName(String kerberosConfigFileName) {
		this.kerberosConfigFileName = kerberosConfigFileName;
	}

	public String getKerberosKeyTabFileName() {
		return kerberosKeyTabFileName;
	}

	public void setKerberosKeyTabFileName(String kerberosKeyTabFileName) {
		this.kerberosKeyTabFileName = kerberosKeyTabFileName;
	}

	public KerberosLogin(){
		
	}
	
	public String getKerberosLoginUser() {
		return kerberosLoginUser;
	}
	public void setKerberosLoginUser(String kerberosLoginUser) {
		this.kerberosLoginUser = kerberosLoginUser;
	}
	public String getKerberosKeyTabContent() {
		return kerberosKeyTabContent;
	}
	public void setKerberosKeyTabContent(String kerberosKeyTabContent) {
		this.kerberosKeyTabContent = kerberosKeyTabContent;
	}
	public String getKerberosRealms() {
		return kerberosRealms;
	}
	public void setKerberosRealms(String kerberosRealms) {
		this.kerberosRealms = kerberosRealms;
	}
	public String getKerberosKdc() {
		return kerberosKdc;
	}
	public void setKerberosKdc(String kerberosKdc) {
		this.kerberosKdc = kerberosKdc;
	}
	public String getKerbersoAdminServer() {
		return kerbersoAdminServer;
	}
	public void setKerbersoAdminServer(String kerbersoAdminServer) {
		this.kerbersoAdminServer = kerbersoAdminServer;
	}
	public String getKerberosPasswordServer() {
		return kerberosPasswordServer;
	}
	public void setKerberosPasswordServer(String kerberosPasswordServer) {
		this.kerberosPasswordServer = kerberosPasswordServer;
	}
	public String getKerberosDomainName() {
		return kerberosDomainName;
	}
	public void setKerberosDomainName(String kerberosDomainName) {
		this.kerberosDomainName = kerberosDomainName;
	}
	public DataSourceMetadata getMetaData() {
		return metaData;
	}

	public void setMetaData(DataSourceMetadata metaData) {
		this.metaData = metaData;
	}
}

