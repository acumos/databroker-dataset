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

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.List;

import org.acumos.datasource.connection.DbUtilitiesV2;
import org.acumos.datasource.exception.CmlpDataSrcException;
import org.acumos.datasource.schema.DataSourceModelGet;
import org.acumos.datasource.schema.DataSourceModelPost;
import org.acumos.datasource.schema.DataSourceModelPut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestPart;

@Service
public class DataSourceServiceV2Impl implements DataSourceServiceV2 {
	
	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	
	@Autowired
	private DbUtilitiesV2 dbUtilities;

	@Override
	public List<String> getDataSourcesList(String user, String authorization, String namespace, String category,
			String dataSourcekey, String textSearch) throws CmlpDataSrcException, IOException {

		List<String> results = null;
		
		try {
			results = dbUtilities.getDataSourceDetails(user, namespace, category, dataSourcekey, textSearch, false, true, authorization);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return results;
	}

	@Override
	public boolean deleteDataSourceDetail(String user, String datasourceKey) throws CmlpDataSrcException, IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String saveDataSourceDetail(String user, String authorization, String codeCloudAuthorization,
			DataSourceModelPost dataSource) throws CmlpDataSrcException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean updateDataSourceDetail(String user, String authorization, String codeCloudAuthorization,
			String datasourceKey, DataSourceModelPut dataSource) throws CmlpDataSrcException, IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public InputStream getDataSourceContents(String user, String authorization, String dataSourceKey,
			String hdfsFilename) throws CmlpDataSrcException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String checkDataSourcesDetails(String user, String authorization, String codeCloudAuthorization,
			DataSourceModelGet datasource, String dataSourceKey, String mode) throws CmlpDataSrcException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean validateDataSourceConnection(String user, String authorization, String codeCloudAuthorization,
			String dataSourceKey) throws CmlpDataSrcException, IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getMetadataContents(String user, String authorization, String dataSourceKey)
			throws CmlpDataSrcException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getDataSourceSamples(String user, String authorization, String dataSourceKey,
			String hdfsFilename) throws CmlpDataSrcException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> kerberosFileUpload(String user, String authorization, String codeCloudAuthorization,
			RequestPart bodyPart1, RequestPart bodyPart2) throws CmlpDataSrcException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean writebackPrediction(String user, String authorization, String datasourceKey, String hdfsFilename,
			String data, String contentType, String includesHeader) throws CmlpDataSrcException, IOException {
		// TODO Auto-generated method stub
		return false;
	}

}
