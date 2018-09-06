package org.acumos.datasource.service;

import java.io.IOException;

import org.acumos.datasource.exception.CmlpDataSrcException;
import org.acumos.datasource.model.JobSubmissionYarn;
import org.acumos.datasource.model.KerberosLogin;
import org.acumos.datasource.model.SparkYarnTestModel;

public interface SparkYarnSvc {

	public void createKerberosKeytab(KerberosLogin objKerberosLogin) throws IOException, CmlpDataSrcException;

	public String getConnectionStatusWithKerberos(JobSubmissionYarn objJobSubmissionYarn) throws IOException, CmlpDataSrcException;

	public String getConnectionStatusWithKerberos(SparkYarnTestModel objSparkYarnTestModel ) throws IOException, CmlpDataSrcException;

}
