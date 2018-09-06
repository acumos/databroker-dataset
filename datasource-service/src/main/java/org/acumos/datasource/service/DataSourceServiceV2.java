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

package org.acumos.datasource.service;

import java.io.InputStream;
import java.util.List;
import java.io.IOException;

import org.acumos.datasource.exception.CmlpDataSrcException;
import org.acumos.datasource.schema.DataSourceModelGet;
import org.acumos.datasource.schema.DataSourceModelPost;
import org.acumos.datasource.schema.DataSourceModelPut;
import org.springframework.web.bind.annotation.RequestPart;


public interface DataSourceServiceV2 {

	
	/**
	 * fetch datasource list from mongoDB
	 * @param user
	 * @param authorization
	 * @param namespace
	 * @param category
	 * @param dataSourcekey
	 * @param textSearch
	 * @return List<String>
	 * @throws CmlpDataSrcException
	 * @throws IOException
	 */
	public List<String> getDataSourcesList(String user,  String authorization, String namespace, String category, String dataSourcekey, String textSearch)
			throws CmlpDataSrcException, IOException;

	/**
	 * delete datasource detail from mongoDB
	 * @param user
	 * @param datasourceKey
	 * @return boolean
	 * @throws CmlpDataSrcException
	 * @throws IOException
	 */
	public boolean deleteDataSourceDetail(String user, String datasourceKey) throws CmlpDataSrcException, IOException;

	
	/**
	 * 
	 * @param user
	 * @param authorization
	 * @param codeCloudAuthorization
	 * @param dataSource
	 * @return String
	 * @throws CmlpDataSrcException
	 * @throws IOException
	 */
	public String saveDataSourceDetail(String user, String authorization, String codeCloudAuthorization,
			DataSourceModelPost dataSource) throws CmlpDataSrcException, IOException;

	/**
	 * update datasource detail into mongoDB
	 * @param user
	 * @param authorization
	 * @param codeCloudAuthorization
	 * @param datasourceKey
	 * @param dataSource
	 * @return boolean
	 * @throws CmlpDataSrcException
	 * @throws IOException
	 */
	public boolean updateDataSourceDetail(String user, String authorization, String codeCloudAuthorization,
			String datasourceKey, DataSourceModelPut dataSource) throws CmlpDataSrcException, IOException;

	
	/**
	 * fetch datasource contents from source
	 * @param user
	 * @param authorization
	 * @param dataSourceKey
	 * @param hdfsFilename
	 * @return InputStream
	 * @throws CmlpDataSrcException
	 * @throws IOException
	 */
	public InputStream getDataSourceContents(String user, String authorization, String dataSourceKey,String hdfsFilename)
			throws CmlpDataSrcException, IOException;


	/**
	 * validate datasource connection
	 * @param user
	 * @param authorization
	 * @param codeCloudAuthorization
	 * @param datasource
	 * @param dataSourceKey
	 * @param mode
	 * @return String
	 * @throws CmlpDataSrcException
	 * @throws IOException
	 */
	public String checkDataSourcesDetails(String user, String authorization, String codeCloudAuthorization,
			DataSourceModelGet datasource, String dataSourceKey, String mode) throws CmlpDataSrcException, IOException;
	
	/**
	 * validate datasource connection
	 * @param user
	 * @param authorization
	 * @param codeCloudAuthorization
	 * @param dataSourceKey
	 * @return boolean
	 * @throws CmlpDataSrcException
	 * @throws IOException
	 */
	public boolean validateDataSourceConnection(String user, String authorization, String codeCloudAuthorization, 
			String dataSourceKey) throws CmlpDataSrcException, IOException;

	/**
	 * fetch datasource metadata contents from mongoDB
	 * @param user
	 * @param authorization
	 * @param dataSourceKey
	 * @return String
	 * @throws CmlpDataSrcException
	 * @throws IOException
	 */
	public String getMetadataContents(String user, String authorization,
			String dataSourceKey) throws CmlpDataSrcException, IOException;
	
	/**
	 * fetch datasource samples
	 * @param user
	 * @param authorization
	 * @param dataSourceKey
	 * @param hdfsFilename
	 * @return InputSteam
	 * @throws CmlpDataSrcException
	 * @throws IOException
	 */
	public InputStream getDataSourceSamples(String user, String authorization, String dataSourceKey,String hdfsFilename)
			throws CmlpDataSrcException, IOException;

	/**
	 * upload kerberos files into hdfs
	 * @param user
	 * @param authorization
	 * @param codeCloudAuthorization
	 * @param bodyPart1
	 * @param bodyPart2
	 * @return List<String>
	 * @throws CmlpDataSrcException
	 * @throws IOException
	 */
	public List<String> kerberosFileUpload(String user, String authorization, String codeCloudAuthorization,
			RequestPart bodyPart1,
			RequestPart bodyPart2) throws CmlpDataSrcException, IOException;

	/**
	 * write back prediction
	 * @param user
	 * @param authorization
	 * @param datasourceKey
	 * @param hdfsFilename
	 * @param data
	 * @param contentType
	 * @param includesHeader
	 * @return boolean
	 * @throws CmlpDataSrcException
	 * @throws IOException
	 */
	public boolean writebackPrediction(String user, String authorization, String datasourceKey, String hdfsFilename, 
			String data, String contentType, String includesHeader) 
			throws CmlpDataSrcException, IOException;
}

