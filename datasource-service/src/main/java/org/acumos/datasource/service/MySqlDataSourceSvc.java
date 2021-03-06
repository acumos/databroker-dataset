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
import java.sql.Connection;
import java.sql.SQLException;

import org.acumos.datasource.exception.DataSrcException;
import org.acumos.datasource.model.MysqlConnectorModel;
import org.acumos.datasource.schema.DataSourceModelGet;

public interface MySqlDataSourceSvc {
	
	/**
	 * Returns MySql connection status for the Connection Model
	 */
	public String getConnectionStatus(MysqlConnectorModel objMysqlConnectorModel, String query) throws ClassNotFoundException, SQLException, IOException, DataSrcException;
	
	/**
	 * Returns MySql connection status for the given input parameters
	 */
	public Connection getConnection(String server, String port, String dbName, String username, String password) throws ClassNotFoundException, IOException, SQLException, DataSrcException;

	/**
	 * Returns Results for an existing datasource
	 */
	public InputStream getResults(String user, String authorization, String namespace, String datasourceKey) throws DataSrcException, IOException, SQLException, ClassNotFoundException;
	
	/**
	 * Returns sample results for an existing datasource
	 */
	public InputStream getSampleResults(String user, String authorization, String namespace, String datasourceKey) throws DataSrcException, IOException, SQLException, ClassNotFoundException;

	/**
	 * Returns sample results for the new datasource.
	 */
	public InputStream getSampleResults(DataSourceModelGet dataSource) throws IOException, ClassNotFoundException, SQLException, DataSrcException, ClassNotFoundException;
	
}
