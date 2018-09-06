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

import org.acumos.datasource.exception.CmlpDataSrcException;
import org.acumos.datasource.model.KerberosLogin;

public interface HiveBatchDataSourceSvc {
	
	public void createKerberosKeytab(KerberosLogin objKerberosLogin) throws IOException, CmlpDataSrcException;

	public String getConnectionStatusWithKerberos(String hostName, String kerberosLoginUser, String port, String kerberosRealm, String query) throws ClassNotFoundException, SQLException, IOException, CmlpDataSrcException;
	
	public String getConnectionStatusWithKerberos(KerberosLogin objKerberosLogin, String hostName, String port, String query) throws ClassNotFoundException, SQLException, IOException, CmlpDataSrcException;

	public Connection getConnection(String hostName, String kerberosLoginUser, String port, String kerberosRealm) throws IOException, ClassNotFoundException, SQLException, CmlpDataSrcException;

	public InputStream getResults(String user, String authorization, String namespace, String datasourceKey, String batchSize) throws CmlpDataSrcException, IOException, ClassNotFoundException, SQLException;

	public InputStream getSampleResults(String user, String authorization, String namespace, String datasourceKey) throws CmlpDataSrcException, IOException, ClassNotFoundException, SQLException;
}
