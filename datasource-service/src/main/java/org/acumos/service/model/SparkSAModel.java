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

/**
 * 
 * @author am375y
 *
 */
public class SparkSAModel {
	
	private String sparkHostName;
	private String port;
	private String sparkDriverMemory;
	private String sparkExecutorMemory;
	private String fatJarLocation;
	private String sparkAppName;
	
	/**
	 * @return the sparkHostName
	 */
	public String getSparkHostName() {
		return sparkHostName;
	}
	/**
	 * @param sparkHostName the sparkHostName to set
	 */
	public void setSparkHostName(String sparkHostName) {
		this.sparkHostName = sparkHostName;
	}
	/**
	 * @return the port
	 */
	public String getPort() {
		return port;
	}
	/**
	 * @param port the port to set
	 */
	public void setPort(String port) {
		this.port = port;
	}
	/**
	 * @return the sparkDriverMemory
	 */
	public String getSparkDriverMemory() {
		return sparkDriverMemory;
	}
	/**
	 * @param sparkDriverMemory the sparkDriverMemory to set
	 */
	public void setSparkDriverMemory(String sparkDriverMemory) {
		this.sparkDriverMemory = sparkDriverMemory;
	}
	/**
	 * @return the sparkExecutorMemory
	 */
	public String getSparkExecutorMemory() {
		return sparkExecutorMemory;
	}
	/**
	 * @param sparkExecutorMemory the sparkExecutorMemory to set
	 */
	public void setSparkExecutorMemory(String sparkExecutorMemory) {
		this.sparkExecutorMemory = sparkExecutorMemory;
	}
	/**
	 * @return the fatJarLocation
	 */
	public String getFatJarLocation() {
		return fatJarLocation;
	}
	/**
	 * @param fatJarLocation the fatJarLocation to set
	 */
	public void setFatJarLocation(String fatJarLocation) {
		this.fatJarLocation = fatJarLocation;
	}
	/**
	 * @return the sparkAppName
	 */
	public String getSparkAppName() {
		return sparkAppName;
	}
	/**
	 * @param sparkAppName the sparkAppName to set
	 */
	public void setSparkAppName(String sparkAppName) {
		this.sparkAppName = sparkAppName;
	}
	
	
}
