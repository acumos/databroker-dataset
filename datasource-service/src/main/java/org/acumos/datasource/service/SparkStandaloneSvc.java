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

import org.acumos.datasource.exception.DataSrcException;
import org.acumos.datasource.model.SparkSAModel;

public interface SparkStandaloneSvc {
	
	/**
	 * Returns Spark connection status for the given hostName and port
	 */
	public String getConnectionStatus(String sparkHostName, String port) throws IOException, InterruptedException, DataSrcException;

	
	/**
	 * Returns Spark connection status for the given SparkSAModel
	 */
	public String getConnectionStatus(SparkSAModel sparkObject) throws IOException, InterruptedException, DataSrcException;

}
