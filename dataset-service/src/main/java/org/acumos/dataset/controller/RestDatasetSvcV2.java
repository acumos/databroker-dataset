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

package org.acumos.dataset.controller;

import javax.ws.rs.core.Response;

import org.acumos.dataset.schema.DataSetSearchKeys;
import org.acumos.dataset.schema.DatasetAttributeMetaData;
import org.acumos.dataset.schema.DatasetDatasourceInfo;
import org.acumos.dataset.schema.DatasetModelPost;
import org.acumos.dataset.schema.DatasetModelPut;

public interface RestDatasetSvcV2 {
	
	/**
	 * fetch dataset list
	 * @param authorization
	 * @param textSearch
	 * @param datasetSearchKeys
	 * @param offset
	 * @param limit
	 * @return Response
	 */
	public Response getDataSetListV2(String authorization, String textSearch, DataSetSearchKeys datasetSearchKeys, int offset,int limit);
	
	
	/**
	 * fetch dataset
	 * @param authorization
	 * @param dataSetKey
	 * @return Response
	 */
	public Response getDataSetV2(String authorization, String dataSetKey);
	
	/**
	 * save dataset
	 * @param authorization
	 * @param dataSetObject
	 * @return Response
	 */
	public Response saveDataSetDetailV2(String authorization, DatasetModelPost dataSetObject);
	
	/**
	 * updae dataset
	 * @param authorization
	 * @param dataSetKey
	 * @param dataSet
	 * @return Response
	 */
	public Response updateDataSetDetailV2(String authorization,String dataSetKey, DatasetModelPut dataSet);
	
	/**
	 * delete dataset
	 * @param authorization
	 * @param dataSetKey
	 * @return Response
	 */
	public Response deleteDataSetDetailV2(String authorization,String dataSetKey);
	
	/**
	 * fetch datasource
	 * @param authorization
	 * @param dataSetKey
	 * @return Response
	 */
	public Response getDataSourcesV2(String authorization,String dataSetKey);
	
	/**
	 * update datasource key
	 * @param authorization
	 * @param dataSetKey
	 * @param dataSource
	 * @return Response
	 */
	public Response updateDataSourceKeyV2(String authorization,String dataSetKey, DatasetDatasourceInfo dataSource);
	
	/**
	 * get dataset attributes metadata
	 * @param authorization
	 * @param dataSetKey
	 * @return Response
	 */
	public Response getAttributeMetaDataV2(String authorization,String dataSetKey);
	
	/**
	 * update dataset attributes metadata
	 * @param authorization
	 * @param dataSetKey
	 * @param attributeMetaData
	 * @return Response
	 */
	public Response updateAttributeMetaDataV2(String authorization,String dataSetKey, DatasetAttributeMetaData attributeMetaData);
}
