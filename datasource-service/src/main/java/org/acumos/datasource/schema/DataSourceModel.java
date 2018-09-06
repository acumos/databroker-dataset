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

package org.acumos.datasource.schema;

import java.io.Serializable;
import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DataSourceModel implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	protected String category;
	protected String namespace;
	protected String datasourceName;
	protected String datasourceDescription;
	protected String readWriteDescriptor;
	protected String predictorKey;
	protected boolean isDataReference;
	
	//common
	protected CommonDetailsInfo commonDetails;
	
	//fileDetails
	protected FileDetailsInfo fileDetails;
	
	//DBDetails
	protected DBDetailsInfo dbDetails;
	
	//hdfs/hive
	protected HdfsHiveDetailsInfo hdfsHiveDetails;
	
	protected ArrayList<NameValue> serviceMetaData;
	protected ArrayList<NameValue> customMetaData;
	
	
	public DataSourceModel() {
		super();
		this.commonDetails = new CommonDetailsInfo();
		this.fileDetails = new FileDetailsInfo();
		this.dbDetails = new DBDetailsInfo();
		this.hdfsHiveDetails = new HdfsHiveDetailsInfo();
		
		this.serviceMetaData = new ArrayList<NameValue>();
		this.customMetaData = new ArrayList<NameValue>();
		
		init();
	}
	
	public void init() {
		this.serviceMetaData.add(new NameValue());
		this.customMetaData.add(new NameValue());
	}
	
	public String getCategory() {
		return category;
	}
	public void setCategory(String category) {
		this.category = category;
	}
	public String getNamespace() {
		return namespace;
	}
	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}
	public String getDatasourceName() {
		return datasourceName;
	}
	public void setDatasourceName(String datasourceName) {
		this.datasourceName = datasourceName;
	}
	public String getDatasourceDescription() {
		return datasourceDescription;
	}
	public void setDatasourceDescription(String datasourceDescription) {
		this.datasourceDescription = datasourceDescription;
	}
	public String getReadWriteDescriptor() {
		return readWriteDescriptor;
	}
	public void setReadWriteDescriptor(String readWriteDescriptor) {
		this.readWriteDescriptor = readWriteDescriptor;
	}
	public String getPredictorKey() {
		return predictorKey;
	}
	public void setPredictorKey(String predictorKey) {
		this.predictorKey = predictorKey;
	}
	public CommonDetailsInfo getCommonDetails() {
		return commonDetails;
	}
	public void setCommonDetails(CommonDetailsInfo commonDetails) {
		this.commonDetails = commonDetails;
	}
	public FileDetailsInfo getFileDetails() {
		return fileDetails;
	}
	public void setFileDetails(FileDetailsInfo fileDetails) {
		this.fileDetails = fileDetails;
	}
	public DBDetailsInfo getDbDetails() {
		return dbDetails;
	}
	public void setDbDetails(DBDetailsInfo dbDetails) {
		this.dbDetails = dbDetails;
	}
	public HdfsHiveDetailsInfo getHdfsHiveDetails() {
		return hdfsHiveDetails;
	}
	public void setHdfsHiveDetails(HdfsHiveDetailsInfo hdfsHiveDetails) {
		this.hdfsHiveDetails = hdfsHiveDetails;
	}
	public ArrayList<NameValue> getServiceMetaData() {
		return serviceMetaData;
	}
	public void setServiceMetaData(ArrayList<NameValue> serviceMetaData) {
		this.serviceMetaData = serviceMetaData;
	}
	public ArrayList<NameValue> getCustomMetaData() {
		return customMetaData;
	}
	public void setCustomMetaData(ArrayList<NameValue> customMetaData) {
		this.customMetaData = customMetaData;
	}

	@JsonProperty("isDataReference")
	public boolean isDataReference() {
		return isDataReference;
	}

	@JsonProperty("isDataReference")
	public void setDataReference(boolean isDataReference) {
		this.isDataReference = isDataReference;
	}
	
	
}
