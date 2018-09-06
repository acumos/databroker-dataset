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

import java.util.ArrayList;

public class JobSubmissionYarn {
	
	private String kerberosLoginUser;
	private String kerberosRealm;
	private String fatJarLocation;
	private String mainClassName;
	private String hadoopHostName;
	private String yarnRMAddress;
	private String yarnRMPrincipal;
	private String sparkYarnJar;
	private String appName;
	private String deployMode;
	private String hdpVersion;
	private String driverMemory;
	private String executorMemory;
	private ArrayList<String> appArguments;
	public String getKerberosLoginUser() {
		return kerberosLoginUser;
	}
	public void setKerberosLoginUser(String kerberosLoginUser) {
		this.kerberosLoginUser = kerberosLoginUser;
	}
	public String getFatJarLocation() {
		return fatJarLocation;
	}
	public void setFatJarLocation(String fatJarLocation) {
		this.fatJarLocation = fatJarLocation;
	}
	public String getMainClassName() {
		return mainClassName;
	}
	public void setMainClassName(String mainClassName) {
		this.mainClassName = mainClassName;
	}
	public String getHadoopHostName() {
		return hadoopHostName;
	}
	public void setHadoopHostName(String hadoopHostName) {
		this.hadoopHostName = hadoopHostName;
	}
	public String getYarnRMAddress() {
		return yarnRMAddress;
	}
	public void setYarnRMAddress(String yarnRMAddress) {
		this.yarnRMAddress = yarnRMAddress;
	}
	public String getYarnRMPrincipal() {
		return yarnRMPrincipal;
	}
	public void setYarnRMPrincipal(String yarnRMPrincipal) {
		this.yarnRMPrincipal = yarnRMPrincipal;
	}
	public String getSparkYarnJar() {
		return sparkYarnJar;
	}
	public void setSparkYarnJar(String sparkYarnJar) {
		this.sparkYarnJar = sparkYarnJar;
	}
	public String getAppName() {
		return appName;
	}
	public void setAppName(String appName) {
		this.appName = appName;
	}
	public String getDeployMode() {
		return deployMode;
	}
	public void setDeployMode(String deployMode) {
		this.deployMode = deployMode;
	}
	public String getHdpVersion() {
		return hdpVersion;
	}
	public void setHdpVersion(String hdpVersion) {
		this.hdpVersion = hdpVersion;
	}
	public String getDriverMemory() {
		return driverMemory;
	}
	public void setDriverMemory(String driverMemory) {
		this.driverMemory = driverMemory;
	}
	public String getExecutorMemory() {
		return executorMemory;
	}
	public void setExecutorMemory(String executorMemory) {
		this.executorMemory = executorMemory;
	}
	public ArrayList<String> getAppArguments() {
		return appArguments;
	}
	public void setAppArguments(ArrayList<String> appArguments) {
		if (appArguments == null){
			appArguments = new ArrayList<>();
		}
		this.appArguments = appArguments;
	}
	public String getKerberosRealm() {
		return kerberosRealm;
	}
	public void setKerberosRealm(String kerberosRealm) {
		this.kerberosRealm = kerberosRealm;
	}

}
