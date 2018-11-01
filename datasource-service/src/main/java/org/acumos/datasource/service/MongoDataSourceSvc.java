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

import org.acumos.datasource.exception.DataSrcException;
import org.acumos.datasource.model.MongoDbConnectionModel;
import org.acumos.datasource.schema.DataSourceModelGet;

import com.mongodb.MongoClient;

public interface MongoDataSourceSvc {
	
	/**
	 * Returns MongoDB connection status for the Connection Model
	 */
	public String getConnectionStatus_2_10(MongoDbConnectionModel objMongoDbConnectionModel, String query) throws IOException, DataSrcException;
	
	/**
	 * Returns MongoDB connection status for the Connection Model
	 */
	public MongoClient getConnection(MongoDbConnectionModel objMongoDbConnectionModel) throws IOException, DataSrcException;
	
	/**
	 * Returns Results for an existing datasource
	 */
	public InputStream getResults(String user, String authorization, String namespace, String datasourceKey) throws DataSrcException, IOException;

	/**
	 * Returns sample results for an existing datasource
	 */
	public InputStream getSampleResults(String user, String authorization, String namespace, String datasourceKey) throws DataSrcException, IOException;

	/**
	 * Returns sample results for the new datasource.
	 */
	public InputStream getSampleResults(DataSourceModelGet dataSource) throws IOException, DataSrcException;

}
