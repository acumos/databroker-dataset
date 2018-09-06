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
import org.acumos.datasource.model.JobSubmissionYarn;
import org.acumos.datasource.model.KerberosLogin;
import org.acumos.datasource.model.SparkYarnTestModel;

public interface SparkYarnSvc {

	/**
	 * Returns void after creating kerberos Keytab for the input kerberos Login
	 */
	public void createKerberosKeytab(KerberosLogin objKerberosLogin) throws IOException, DataSrcException;

	/**
	 * Returns Spark Yarn connection status for the given Job Submission
	 */
	public String getConnectionStatusWithKerberos(JobSubmissionYarn objJobSubmissionYarn) throws IOException, DataSrcException;

	/**
	 * Returns Spark connection status for the given Spark Yarn Test Model
	 */
	public String getConnectionStatusWithKerberos(SparkYarnTestModel objSparkYarnTestModel ) throws IOException, DataSrcException;

}
