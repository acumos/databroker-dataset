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

package org.acumos.service.controller;

import javax.ws.rs.core.Response;

import org.acumos.service.schema.DataSetSearchKeys;
import org.acumos.service.schema.DatasetAttributeMetaData;
import org.acumos.service.schema.DatasetDatasourceInfo;
import org.acumos.service.schema.DatasetModelPost;
import org.acumos.service.schema.DatasetModelPut;

public interface RestDatasetSvcV2 {
	public Response getDataSetListV2(String authorization, String textSearch, DataSetSearchKeys datasetSearchKeys, int offset,int limit);
	public Response getDataSetV2(String authorization, String dataSetKey);
	public Response saveDataSetDetailV2(String authorization, DatasetModelPost dataSetObject);
	public Response updateDataSetDetailV2(String authorization,String dataSetKey, DatasetModelPut dataSet);
	public Response deleteDataSetDetailV2(String authorization,String dataSetKey);
	public Response getDataSourcesV2(String authorization,String dataSetKey);
	public Response updateDataSourceKeyV2(String authorization,String dataSetKey, DatasetDatasourceInfo dataSource);
	public Response getAttributeMetaDataV2(String authorization,String dataSetKey);
	public Response updateAttributeMetaDataV2(String authorization,String dataSetKey, DatasetAttributeMetaData attributeMetaData);
}
