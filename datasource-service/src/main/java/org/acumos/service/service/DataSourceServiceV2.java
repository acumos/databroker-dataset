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

import java.io.InputStream;
import java.util.List;
import java.io.IOException;

import org.acumos.service.exception.CmlpDataSrcException;
import org.acumos.service.schema.DataSourceModelGet;
import org.acumos.service.schema.DataSourceModelPost;
import org.acumos.service.schema.DataSourceModelPut;
import org.springframework.web.bind.annotation.RequestPart;


public interface DataSourceServiceV2 {

	
	public List<String> getDataSourcesList(String user,  String authorization, String namespace, String category, String dataSourcekey, String textSearch)
			throws CmlpDataSrcException, IOException;

	public boolean deleteDataSourceDetail(String user, String datasourceKey) throws CmlpDataSrcException, IOException;

	
	public String saveDataSourceDetail(String user, String authorization, String codeCloudAuthorization,
			DataSourceModelPost dataSource) throws CmlpDataSrcException, IOException;

	
	public boolean updateDataSourceDetail(String user, String authorization, String codeCloudAuthorization,
			String datasourceKey, DataSourceModelPut dataSource) throws CmlpDataSrcException, IOException;

	
	public InputStream getDataSourceContents(String user, String authorization, String dataSourceKey,String hdfsFilename)
			throws CmlpDataSrcException, IOException;


	
	public String checkDataSourcesDetails(String user, String authorization, String codeCloudAuthorization,
			DataSourceModelGet datasource, String dataSourceKey, String mode) throws CmlpDataSrcException, IOException;
	
	
	public boolean validateDataSourceConnection(String user, String authorization, String codeCloudAuthorization, 
			String dataSourceKey) throws CmlpDataSrcException, IOException;

	
	public String getMetadataContents(String user, String authorization,
			String dataSourceKey) throws CmlpDataSrcException, IOException;
	
	
	public InputStream getDataSourceSamples(String user, String authorization, String dataSourceKey,String hdfsFilename)
			throws CmlpDataSrcException, IOException;

	
	public List<String> kerberosFileUpload(String user, String authorization, String codeCloudAuthorization,
			RequestPart bodyPart1,
			RequestPart bodyPart2) throws CmlpDataSrcException, IOException;

	
	public boolean writebackPrediction(String user, String authorization, String datasourceKey, String hdfsFilename, 
			String data, String contentType, String includesHeader) 
			throws CmlpDataSrcException, IOException;
}

