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

package org.acumos.service.service;

import java.io.IOException;
import java.util.List;

import org.acumos.service.exception.CmlpDataSrcException;
import org.acumos.service.schema.DataSetSearchKeys;
import org.acumos.service.schema.DatasetAttributeMetaData;
import org.acumos.service.schema.DatasetModelPost;
import org.acumos.service.schema.DatasetModelPut;
import org.apache.http.entity.mime.FormBodyPart;
import org.json.JSONException;

public interface DataSetServiceV2 {
	
	public String insertDataSet(String authorization, String user, DatasetModelPost datasetModel) throws CmlpDataSrcException, IOException, JSONException;
	public List<String> getDataSetDetails(String user, String datasetKey, String searchKey, DataSetSearchKeys dataSearchKeys) throws CmlpDataSrcException, IOException;
	public boolean updateDataSetDetail(String authorization, String user, String datasetKey, DatasetModelPut datasetModel) throws CmlpDataSrcException, IOException, JSONException; 
	public boolean deleteDataSetDetail(String user, String datasetKey) throws CmlpDataSrcException, IOException;
	public String getDataSources(String authorization, String user, String datasetKey) throws CmlpDataSrcException, IOException, JSONException;
	public boolean updateAttributeMetaData(String authorization, String user, String datasetKey, DatasetAttributeMetaData attributeMetaData) throws CmlpDataSrcException, IOException; 
	public boolean updateDataSourceKey(String authorization, String user, String datasetKey, String datasourceKey) throws CmlpDataSrcException, IOException;
	public void writebackPrediction(String user, String authorization, String datasetKey, FormBodyPart dataFile) throws CmlpDataSrcException, IOException, JSONException;
}