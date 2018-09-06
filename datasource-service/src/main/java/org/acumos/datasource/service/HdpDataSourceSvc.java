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

import org.apache.hadoop.conf.Configuration;

import org.acumos.datasource.exception.CmlpDataSrcException;
import org.acumos.datasource.model.KerberosLogin;
import org.acumos.datasource.schema.DataSourceModelGet;
import org.acumos.datasource.schema.DataSourceMetadata;

public interface HdpDataSourceSvc {

	public void createKerberosKeytab(KerberosLogin objKerberosLogin) throws IOException, CmlpDataSrcException;

	public String getConnStatusWithKerberos(KerberosLogin objKerberosLogin, String hostName,String readWriteDescriptor) throws IOException, CmlpDataSrcException;

	public String getConnStatusWithKerberos(KerberosLogin objKerberosLogin, String hostName, String hdfsFolderName,String readWriteDescriptor) throws IOException, CmlpDataSrcException;

	public String getConnStatusWithKerberos(String hostName, String kerberosLoginuser, String hdfsFolderName,String readWriteDescriptor) throws IOException, CmlpDataSrcException;

	public String getConnStatusWithoutKerberos(String hostName) throws IOException, CmlpDataSrcException;

	public Configuration createConnWithoutKerberos(String hostName) throws CmlpDataSrcException, IOException;

	public InputStream getResults(String user, String authorization, String namespace, String datasourceKey,String hdfsFilename) throws CmlpDataSrcException, IOException;

	public InputStream getSampleResults(String user, String authorization, String namespace, String datasourceKey,String hdfsFilename) throws CmlpDataSrcException, IOException;

	public DataSourceMetadata getMetadata(KerberosLogin objKerberosLogin, String hostName, String hdfsFolderName) throws IOException, CmlpDataSrcException;

	public InputStream getSampleResults(DataSourceModelGet dataSource) throws CmlpDataSrcException, IOException;

	boolean writebackPrediction(String user, String authorization, DataSourceModelGet dataSource, String data,String hdfsFilename)
			throws IOException, CmlpDataSrcException;

}
