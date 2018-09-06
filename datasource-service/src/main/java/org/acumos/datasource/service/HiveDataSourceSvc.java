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
import org.acumos.datasource.model.KerberosLogin;
import org.acumos.datasource.schema.DataSourceModelGet;

public interface HiveDataSourceSvc {
	
	/**
	 * Returns void after creating kerberos Keytab for the input kerberos Login
	 */
	public void createKerberosKeytab(KerberosLogin objKerberosLogin) throws IOException, DataSrcException;

	/**
	 * Returns connection status for the given kerberos input parameters
	 */
	public String getConnectionStatusWithKerberos(String hostName, String kerberosLoginUser, String port, String kerberosRealm, String query) throws ClassNotFoundException, SQLException, IOException, DataSrcException;

	/**
	 * Returns connection status for the given kerberos input parameters
	 */	
	public String getConnectionStatusWithKerberos(KerberosLogin objKerberosLogin, String hostName, String port, String query) throws ClassNotFoundException, SQLException, IOException, DataSrcException;

	/**
	 * Returns connection for the given kerberos input parameters
	 */
	public Connection getConnection(String hostName, String kerberosLoginUser, String port, String kerberosRealm) throws IOException, ClassNotFoundException, SQLException, DataSrcException;

	/**
	 * Returns Results for an existing datasource
	 */
	public InputStream getResults(String user, String authorization, String namespace, String datasourceKey) throws DataSrcException, IOException, SQLException, ClassNotFoundException;

	/**
	 * Returns Sample Results for an existing datasource
	 */
	public InputStream getSampleResults(String user, String authorization, String namespace, String datasourceKey) throws DataSrcException, IOException, SQLException, ClassNotFoundException;

	/**
	 * Returns Results for the new datasource
	 */
	public InputStream getSampleResults(DataSourceModelGet dataSource) throws DataSrcException, IOException, SQLException, ClassNotFoundException;
}
