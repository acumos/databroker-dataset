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

import org.acumos.datasource.exception.CmlpDataSrcException;
import org.acumos.datasource.model.CassandraConnectionModel;
import org.acumos.datasource.schema.DataSourceMetadata;
import org.acumos.datasource.schema.DataSourceModelGet;

import com.datastax.driver.core.Session;

public interface CassandraDataSourceSvc {
	
	public String getConnectionStatus(String node, int port, String keyspaceName) throws CmlpDataSrcException;
	
	public void createkeySpace(Session session, String keySpacename, String replicationStrategy, int replicationFactor) throws CmlpDataSrcException;
	
	public String getConnectionStatus(CassandraConnectionModel objCassandraConnectionModel, String query) throws CmlpDataSrcException, IOException, SQLException;
	
	public InputStream getResults(String user, String authorization, String namespace, String datasourceKey) throws CmlpDataSrcException, IOException;
	
	public InputStream getSampleResults(String user, String authorization, String namespace, String datasourceKey) throws CmlpDataSrcException, IOException;
	
	public Session getSession(String node, int port, String username, String password) throws CmlpDataSrcException;
	
	public DataSourceMetadata getMetadataResults(String user, String authorization, String datasourceKey) throws CmlpDataSrcException, IOException, SQLException;

	public InputStream getSampleResults(DataSourceModelGet dataSource) throws CmlpDataSrcException, IOException;
	
}
