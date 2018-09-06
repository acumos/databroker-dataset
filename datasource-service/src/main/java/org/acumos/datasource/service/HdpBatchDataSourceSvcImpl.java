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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import org.slf4j.LoggerFactory;
import org.acumos.datasource.common.CmlpApplicationEnum;
import org.acumos.datasource.common.CmlpErrorList;
import org.acumos.datasource.common.CmlpRestError;
import org.acumos.datasource.common.ErrorListEnum;
import org.acumos.datasource.common.HelperTool;
import org.acumos.datasource.common.Utilities;
import org.acumos.datasource.connection.DbUtilitiesV2;
import org.acumos.datasource.exception.CmlpDataSrcException;
import org.acumos.datasource.model.KerberosLogin;
import org.acumos.datasource.schema.DataSourceMetadata;
import org.acumos.datasource.schema.DataSourceModelGet;
import org.acumos.datasource.schema.NameValue;
import org.slf4j.Logger;

import javax.ws.rs.core.Response.Status;

@Service
public class HdpBatchDataSourceSvcImpl implements HdpBatchDataSourceSvc {

	private static Logger log = LoggerFactory.getLogger(HdpBatchDataSourceSvcImpl.class);

	public HdpBatchDataSourceSvcImpl() {
	}

	@Autowired
	private DbUtilitiesV2 dbUtilities;

	@Override
	public Configuration createConnWithoutKerberos(String hostName) throws CmlpDataSrcException, IOException {
		log.info("Create a hadoop configuration without kerberos for " + hostName);
		Configuration config = new Configuration();
		config.set("fs.DefaultFS", hostName);
		log.info("Hadoop configuration without kerberos with fs.DefaultFS as " + hostName);
		return config;
	}

	@Override
	public String getConnStatusWithKerberos(KerberosLogin objKerberosLogin, String hostName) throws IOException, CmlpDataSrcException {
		log.info("Checking kerberos keytab");
		if (!(new File(
				System.getProperty("user.dir") + System.getProperty("file.separator")
						+ objKerberosLogin.getKerberosLoginUser().substring(0,
								objKerberosLogin.getKerberosLoginUser().indexOf("@"))
						+ "." + "kerberos.keytab").exists())) {
			log.info("Kerberos keytab doesn't exist, creating a new one");
			Utilities.createKerberosKeytab(objKerberosLogin);
		}
		log.info("Creating a hadoop configuration with kerberos for " + hostName);
		String result = getConnStatusWithKerberos(hostName, objKerberosLogin.getKerberosLoginUser(), null);
		return result;
	}

	@Override
	public String getConnStatusWithoutKerberos(String hostName) throws IOException, CmlpDataSrcException {
		log.info("Starting to establish a test connection to " + hostName + " for hdfs connectivity.");
		log.info("Creating hadoop configuration for " + hostName);
		Configuration config = createConnWithoutKerberos(hostName);
		log.info("Initializing filesystem with kerberised settings for " + hostName + " for hdfs connectivity.");
		FileSystem fs = FileSystem.get(config);
		log.info("Reading filesystem with kerberised settings for " + hostName + " for hdfs connectivity.");
		FileStatus[] fsStatus = fs.listStatus(new Path("/tmp"));
		if (fsStatus.length > 0) {
			log.info("Reading of temp folder succeeded for establishing hdfs connectivity on " + hostName);
			return "success";
		}
		return "failed";
	}

	@Override
	public void createKerberosKeytab(KerberosLogin objKerberosLogin) throws IOException, CmlpDataSrcException {
		log.info("Creating kerberos keytab for principal " + objKerberosLogin.getKerberosLoginUser());
		Utilities.createKerberosKeytab(objKerberosLogin);
		log.info("Created kerberos keytab for principal " + objKerberosLogin.getKerberosLoginUser());
	}

	@Override
	public String getConnStatusWithKerberos(String hostName, String kerberosLoginuser, String hdfsFolderName)
			throws IOException, CmlpDataSrcException {
		log.info("Starting to establish a test connection to " + hostName + " for hdfs connectivity.");
		Configuration config = HelperTool.getKerberisedConfiguration(hostName,
				kerberosLoginuser);
		log.info("Initializing filesystem with kerberised settings for " + hostName + " for hdfs connectivity.");
		FileSystem fs = FileSystem.get(config);
		log.info("Reading filesystem with kerberised settings for " + hostName + " for hdfs connectivity.");
		if (hdfsFolderName != null) {
			FSDataInputStream in = fs.open(new Path(hdfsFolderName));
			if (in.available() > 0) {
				log.info("Reading of " + hdfsFolderName + " succeeded for establishing hdfs connectivity on "
						+ hostName);
				in.close();
				return "success";
			}
		} else {
			FileStatus[] fsStatus = fs.listStatus(new Path("/tmp"));
			if (fsStatus.length > 0) {
				log.info("Reading of temp folder succeeded for establishing hdfs connectivity on " + hostName);
				return "success";
			}
		}

		return "failed";
	}

	@Override
	public String getConnStatusWithKerberos(KerberosLogin objKerberosLogin, String hostName, String hdfsFolderName)
			throws IOException, CmlpDataSrcException {
		log.info("Checking kerberos keytab");
		if (!(new File(
				System.getProperty("user.dir") + System.getProperty("file.separator")
						+ objKerberosLogin.getKerberosLoginUser().substring(0,
								objKerberosLogin.getKerberosLoginUser().indexOf("@"))
						+ "." + "kerberos.keytab").exists())) {
			log.info("Kerberos keytab doesn't exist, creating a new one");
			Utilities.createKerberosKeytab(objKerberosLogin);
		}
		log.info("Creating a hadoop configuration with kerberos for " + hostName);
		String result = getConnStatusWithKerberos(hostName, objKerberosLogin.getKerberosLoginUser(), hdfsFolderName);
		return result;
	}

	@Override
	public InputStream getResults(String user, String authorization, String namespace, String datasourceKey, String batchSize)
			throws CmlpDataSrcException, IOException {
		
		ArrayList<String> dbDatasourceDetails = dbUtilities.getDataSourceDetails(user, null, null, datasourceKey, null, true, false, authorization);
		//JSONObject objJson = new JSONObject(cassandraDetails.get(0));
		
		DataSourceModelGet dbDataSource = Utilities.getDataSourceModel(dbDatasourceDetails.get(0));
		
		if (dbDataSource.getCategory().equals("hdfs batch") && dbDataSource.getOwnedBy().equals(user)) {
			Map<String, String> decryptionMap = new HashMap<>();
			//decryptionMap = Utilities.readFromCodeCloud(authorization, datasourceKey);
			decryptionMap = Utilities.readFromMongoCodeCloud(user, datasourceKey);

			log.info("Starting to establish a test connection to " + dbDataSource.getCommonDetails().getServerName()
					+ " for hdfs connectivity.");
			Configuration config = HelperTool.getKerberisedConfiguration(
					dbDataSource.getCommonDetails().getServerName(), decryptionMap.get("kerberosLoginUser".toLowerCase()));
			log.info("Initializing filesystem with kerberised settings for " + dbDataSource.getCommonDetails().getServerName()
					+ " for hdfs connectivity.");
			FileSystem fs = FileSystem.get(config);
			log.info("Reading filesystem with kerberised settings for " + dbDataSource.getCommonDetails().getServerName()
					+ " for hdfs connectivity.");
			if (dbDataSource.getHdfsHiveDetails().getHdfsFoldername() != null) {
				FSDataInputStream in = fs.open(new Path(dbDataSource.getHdfsHiveDetails().getHdfsFoldername()), Integer.parseInt(dbDataSource.getHdfsHiveDetails().getBatchSize()));
				if (in.available() > 0) {
					log.info("Reading of " + dbDataSource.getHdfsHiveDetails().getHdfsFoldername()
							+ " succeeded for establishing hdfs connectivity on "
							+ dbDataSource.getCommonDetails().getServerName());
					int limit =  Integer.parseInt(dbDataSource.getHdfsHiveDetails().getBatchSize()) * 100;
					byte[] buffer = new byte[limit];
					int bytesRead = in.read(100l, buffer, 0, limit);
					in.close();
					
					return (new  ByteArrayInputStream(buffer));
				}
			}
		}
		
		//No Results found for the given DatasourceKey
		String[] variables = {"datasourceKey"};
				
		CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
		
		throw new CmlpDataSrcException("No Results found for the given DatasourceKey.",
				Status.NOT_FOUND.getStatusCode(), err);
	}
	
	
	@Override
	public InputStream getSampleResults(String user, String authorization, String namespace, String datasourceKey)
			throws CmlpDataSrcException, IOException {
		
		ArrayList<String> dbDatasourceDetails = dbUtilities.getDataSourceDetails(user, null, null, datasourceKey, null, true, false, authorization);
		//JSONObject objJson = new JSONObject(hdfsDatasourceDetails.get(0));
		
		DataSourceModelGet dbDataSource = Utilities.getDataSourceModel(dbDatasourceDetails.get(0));
		
		if (dbDataSource.getCategory().equals("hdfs batch") && dbDataSource.getOwnedBy().equals(user)) {
			Map<String, String> decryptionMap = new HashMap<>();
			//decryptionMap = Utilities.readFromCodeCloud(authorization, datasourceKey);
			decryptionMap = Utilities.readFromMongoCodeCloud(user, datasourceKey);
			
			log.info("Starting to establish a test connection to " + dbDataSource.getCommonDetails().getServerName()
					+ " for hdfs connectivity.");
			Configuration config = HelperTool.getKerberisedConfiguration(
					dbDataSource.getCommonDetails().getServerName(), decryptionMap.get("kerberosLoginUser".toLowerCase()));	
				//	kerberosConfigFileName, kerberosKeyTabFileName);
			
			log.info("Initializing filesystem with kerberised settings for " + dbDataSource.getCommonDetails().getServerName()
					+ " for hdfs connectivity.");
			FileSystem fs = FileSystem.get(config);
			
			//connectivity test passed
			if("write".equalsIgnoreCase(dbDataSource.getReadWriteDescriptor())) {
				//validate connection call as there is no getSamples() allowed on write flag
				return (new  ByteArrayInputStream("Success".getBytes()));
			}
			
			log.info("Reading filesystem with kerberised settings for " + dbDataSource.getCommonDetails().getServerName()
					+ " for hdfs connectivity.");
			if (dbDataSource.getHdfsHiveDetails().getHdfsFoldername() != null) {
				FSDataInputStream in = fs.open(new Path(dbDataSource.getHdfsHiveDetails().getHdfsFoldername()));
				if (in.available() > 0) {
					log.info("Reading of " + dbDataSource.getHdfsHiveDetails().getHdfsFoldername()
							+ " succeeded for establishing hdfs connectivity on "
							+ dbDataSource.getCommonDetails().getServerName());
					int limit = Integer.parseInt(HelperTool.getEnv("datasource_sample_size",
							HelperTool.getComponentPropertyValue("datasource_sample_size"))) * 100;
					byte[] buffer = new byte[limit];
					int bytesRead = in.read(0l, buffer, 0, limit);
					return (new  ByteArrayInputStream(buffer));
					//return in.getWrappedStream();
				}
			}
		} 
		
		//No sample information available for the given DatasourceKey
		String[] variables = {"datasourceKey"};
				
		CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
		
		throw new CmlpDataSrcException("No sample information available for the given DatasourceKey.",
				Status.NOT_FOUND.getStatusCode(), err);
	}

	@Override
	public DataSourceMetadata getMetadata(KerberosLogin objKerberosLogin, String hostName, String hdfsFolderName) throws IOException, CmlpDataSrcException {
		ArrayList<NameValue> metaDataList = new ArrayList<NameValue>();
		DataSourceMetadata metadata = new DataSourceMetadata();
		log.info("Checking kerberos keytab");
		if (!(new File(
				System.getProperty("user.dir") + System.getProperty("file.separator")
						+ objKerberosLogin.getKerberosLoginUser().substring(0,
								objKerberosLogin.getKerberosLoginUser().indexOf("@"))
						+ "." + "kerberos.keytab").exists())) {
			log.info("Kerberos keytab doesn't exist, creating a new one");
			Utilities.createKerberosKeytab(objKerberosLogin);
		}
		log.info("Creating a hadoop configuration with kerberos for " + hostName);
		log.info("Starting to establish a test connection to " + hostName + " for hdfs connectivity.");
		Configuration config = HelperTool.getKerberisedConfiguration(hostName,
				objKerberosLogin.getKerberosLoginUser());
		log.info("Initializing filesystem with kerberised settings for " + hostName + " for hdfs connectivity.");
		FileSystem fs = FileSystem.get(config);
		log.info("Reading filesystem with kerberised settings for " + hostName + " for hdfs connectivity.");
		if (hdfsFolderName != null) {
			metaDataList.add(new NameValue("fileName", fs.getUri().toString() + hdfsFolderName));
			metaDataList.add(new NameValue("fileLength", String.valueOf(fs.getContentSummary(new Path(hdfsFolderName)).getSpaceConsumed()) + " bytes"));
		}
		metadata.setMetaDataInfo(metaDataList);
		return metadata;
	}

}
