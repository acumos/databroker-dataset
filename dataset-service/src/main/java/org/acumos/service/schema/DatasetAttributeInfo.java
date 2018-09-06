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

public class DatasetAttributeInfo {
	
	protected String attributeType; // Categorical, Numerical, Mixed
	protected String geoStatisticalType; //Multivariate, Univariate, Sequential, Time Series, Text, Domain-Theory, Other
	protected String datasetType; //Training, Testing, Validation, Evaluation
	protected String domainArea; // Domain1, Domain2, etc
	protected String formatType; // Matrix, Non-Matrix
	protected String taskType; //Classification, Regression, Clustering, Other
	
	public String getAttributeType() {
		return attributeType;
	}
	public void setAttributeType(String attributeType) {
		this.attributeType = attributeType;
	}
	public String getGeoStatisticalType() {
		return geoStatisticalType;
	}
	public void setGeoStatisticalType(String geoStatisticalType) {
		this.geoStatisticalType = geoStatisticalType;
	}
	public String getDatasetType() {
		return datasetType;
	}
	public void setDatasetType(String datasetType) {
		this.datasetType = datasetType;
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
	public String getTaskType() {
		return taskType;
	}
	public void setTaskType(String taskType) {
		this.taskType = taskType;
	}
	
}
