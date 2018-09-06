package org.acumos.datasource.service;

import java.io.IOException;

import org.acumos.datasource.exception.CmlpDataSrcException;
import org.acumos.datasource.model.SparkSAModel;

public interface SparkStandaloneSvc {

	public String getConnectionStatus(String spakHostName, String port) throws IOException, InterruptedException, CmlpDataSrcException;

	public String getConnectionStatus(SparkSAModel sparkObject) throws IOException, InterruptedException, CmlpDataSrcException;

}
