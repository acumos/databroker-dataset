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

package org.acumos.dataset.service;

import java.io.IOException;
import java.util.List;

import org.acumos.dataset.exception.DataSetException;
import org.acumos.dataset.schema.DataSetSearchKeys;
import org.acumos.dataset.schema.DatasetAttributeMetaData;
import org.acumos.dataset.schema.DatasetModelPost;
import org.acumos.dataset.schema.DatasetModelPut;
import org.apache.http.entity.mime.FormBodyPart;
import org.json.JSONException;

public interface DataSetServiceV2 {
	
	/**
	 * insert dataset into monngoDB
	 * @param authorization
	 * @param user
	 * @param datasetModel
	 * @return String
	 * @throws DataSetException
	 * @throws IOException
	 * @throws JSONException
	 */
	public String insertDataSet(String authorization, String user, DatasetModelPost datasetModel) throws DataSetException, IOException, JSONException;
	
	/**
	 * fetch dataset detail from mongoDb
	 * @param user
	 * @param datasetKey
	 * @param searchKey
	 * @param dataSearchKeys
	 * @return List
	 * @throws DataSetException
	 * @throws IOException
	 */
	public List<String> getDataSetDetails(String user, String datasetKey, String searchKey, DataSetSearchKeys dataSearchKeys) throws DataSetException, IOException;
	
	/**
	 * update dataset into mongoDB
	 * @param authorization
	 * @param user
	 * @param datasetKey
	 * @param datasetModel
	 * @return boolean
	 * @throws DataSetException
	 * @throws IOException
	 * @throws JSONException
	 */
	public boolean updateDataSetDetail(String authorization, String user, String datasetKey, DatasetModelPut datasetModel) throws DataSetException, IOException, JSONException; 
	
	/**
	 * delete dataset from mongoDB
	 * @param user
	 * @param datasetKey
	 * @return boolean
	 * @throws DataSetException
	 * @throws IOException
	 */
	public boolean deleteDataSetDetail(String user, String datasetKey) throws DataSetException, IOException;
	
	/**
	 * fetch datasource by datasetKey from mongoDB
	 * @param authorization
	 * @param user
	 * @param datasetKey
	 * @return String
	 * @throws DataSetException
	 * @throws IOException
	 * @throws JSONException
	 */
	public String getDataSources(String authorization, String user, String datasetKey) throws DataSetException, IOException, JSONException;
	
	/**
	 * update dataset attributes into mongoDB
	 * @param authorization
	 * @param user
	 * @param datasetKey
	 * @param attributeMetaData
	 * @return boolean
	 * @throws DataSetException
	 * @throws IOException
	 */
	public boolean updateAttributeMetaData(String authorization, String user, String datasetKey, DatasetAttributeMetaData attributeMetaData) throws DataSetException, IOException; 
	
	/**
	 * update datasource by datasetKey
	 * @param authorization
	 * @param user
	 * @param datasetKey
	 * @param datasourceKey
	 * @return boolean
	 * @throws DataSetException
	 * @throws IOException
	 */
	public boolean updateDataSourceKey(String authorization, String user, String datasetKey, String datasourceKey) throws DataSetException, IOException;
	
	/**
	 * update dataset for write back prediction
	 * @param user
	 * @param authorization
	 * @param datasetKey
	 * @param dataFile
	 * @throws DataSetException
	 * @throws IOException
	 * @throws JSONException
	 */
	public void writebackPrediction(String user, String authorization, String datasetKey, FormBodyPart dataFile) throws DataSetException, IOException, JSONException;
}