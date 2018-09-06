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
import java.util.List;

import org.acumos.datasource.exception.CmlpDataSrcException;
import org.acumos.datasource.model.JdbcConnectionModel;
import org.acumos.datasource.schema.DataSourceModelGet;

public interface JdbcDataSourceSvc {
	

	public String getConnectionStatus(JdbcConnectionModel objJdbcConnectionModel, String sqlStatement, String getReadWriteFlag) 
			throws ClassNotFoundException, SQLException, IOException, CmlpDataSrcException;

	public Connection getConnection(String jdbcURL, String username, String password) throws ClassNotFoundException, IOException, SQLException, CmlpDataSrcException;
	
	public InputStream getResults(String user, String authorization, String namespace, String datasourceKey) throws CmlpDataSrcException, IOException, SQLException, ClassNotFoundException;

	public InputStream getSampleResults(String user, String authorization, String namespace, String datasourceKey) throws CmlpDataSrcException, IOException, SQLException, ClassNotFoundException;

	public InputStream getSampleResults(DataSourceModelGet dataSource) throws IOException, ClassNotFoundException, SQLException, CmlpDataSrcException;

	public boolean writebackPrediction(String user, String authorization, DataSourceModelGet dataSource, String[] headerRow, List<String[]> content) 
			throws CmlpDataSrcException, IOException, ClassNotFoundException;
	
}
