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
import java.sql.SQLException;

import org.acumos.datasource.exception.DataSrcException;
import org.acumos.datasource.model.CassandraConnectionModel;
import org.acumos.datasource.schema.DataSourceMetadata;
import org.acumos.datasource.schema.DataSourceModelGet;

import com.datastax.driver.core.Session;

public interface CassandraDataSourceSvc {
	
	/**
	 * Returns connection status for the given input
	 * @param node
	 * @param port
	 * @param keyspaceName
	 * @return String
	 * @throws DataSrcException
	 */
	public String getConnectionStatus(String node, int port, String keyspaceName) throws DataSrcException;
	
	/**
	 * Returns void after creating keyspace for the given input
	 * @param session
	 * @param keySpacename
	 * @param replicationStrategy
	 * @param replicationFactor
	 * @throws DataSrcException
	 */
	public void createkeySpace(Session session, String keySpacename, String replicationStrategy, int replicationFactor) throws DataSrcException;

	/**
	 * Returns connection status for the given Connection Model
	 * @param objCassandraConnectionModel
	 * @param query
	 * @return String
	 * @throws DataSrcException
	 * @throws IOException
	 * @throws SQLException
	 */
	public String getConnectionStatus(CassandraConnectionModel objCassandraConnectionModel, String query) throws DataSrcException, IOException, SQLException;
	
	/**
	 * Returns Results for an existing datasource
	 * @param user
	 * @param authorization
	 * @param namespace
	 * @param datasourceKey
	 * @return InputStream
	 * @throws DataSrcException
	 * @throws IOException
	 */
	public InputStream getResults(String user, String authorization, String namespace, String datasourceKey) throws DataSrcException, IOException;
	
	/**
	 * Returns Sample Results for an existing datasource
	 * @param user
	 * @param authorization
	 * @param namespace
	 * @param datasourceKey
	 * @return InputStream
	 * @throws DataSrcException
	 * @throws IOException
	 */
	public InputStream getSampleResults(String user, String authorization, String namespace, String datasourceKey) throws DataSrcException, IOException;
	
	/**
	 * Returns Session for the given input
	 * @param node
	 * @param port
	 * @param username
	 * @param password
	 * @return Session
	 * @throws DataSrcException
	 */
	public Session getSession(String node, int port, String username, String password) throws DataSrcException;
	
	/**
	 * Returns Metadata for an existing datasource
	 * @param user
	 * @param authorization
	 * @param datasourceKey
	 * @return DataSourceMetadata
	 * @throws DataSrcException
	 * @throws IOException
	 * @throws SQLException
	 */
	public DataSourceMetadata getMetadataResults(String user, String authorization, String datasourceKey) throws DataSrcException, IOException, SQLException;

	/**
	 * Returns Sample Results for the new datasource
	 * @param dataSource
	 * @return InputStream
	 * @throws DataSrcException
	 * @throws IOException
	 */
	public InputStream getSampleResults(DataSourceModelGet dataSource) throws DataSrcException, IOException;
	
}
