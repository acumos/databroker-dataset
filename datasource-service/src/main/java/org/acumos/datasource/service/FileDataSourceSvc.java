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
import java.net.MalformedURLException;
import java.net.URLConnection;

import org.acumos.datasource.exception.DataSrcException;
import org.acumos.datasource.model.FileConnectionModel;
import org.acumos.datasource.schema.DataSourceModelGet;

public interface FileDataSourceSvc {
	
	/**
	 * Returns URLConnection for the provided input
	 */
	public URLConnection getURLConnection(String fileURL, String username, String password) throws MalformedURLException, IOException, Exception;

	/**
	 * Returns URLConnection for an existing datasource
	 */
	public URLConnection getConnection(String user, String authorization, String namespace, String datasourceKey) throws MalformedURLException, IOException, Exception;

	/**
	 * Returns connection status for the provided connection model
	 */
	public String getConnectionStatus(FileConnectionModel objFileConnectionModel) throws DataSrcException;
	
	/**
	 * Returns Results for an existing datasource
	 */
	public InputStream getResults(String user, String authorization, String namespace, String datasourceKey) throws DataSrcException, Exception;

	/**
	 * Returns Sample Results for an existing datasource
	 */
	public InputStream getSampleResults(String user, String authorization, String namespace, String datasourceKey) throws DataSrcException, Exception;

	/**
	 * Returns Sample Results for the new datasource
	 */
	public InputStream getSampleResults(DataSourceModelGet dataSource) throws MalformedURLException, IOException, DataSrcException;

	/**
	 * Returns boolean after writeback prediction results to the provide 'write' datasource
	 */
	public boolean writebackPrediction(String authorization, DataSourceModelGet dataSource, String data)throws DataSrcException;

}
