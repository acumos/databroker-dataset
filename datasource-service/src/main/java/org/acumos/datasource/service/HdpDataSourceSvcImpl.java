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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.Response.Status;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import org.acumos.datasource.common.CmlpApplicationEnum;
import org.acumos.datasource.common.CmlpErrorList;
import org.acumos.datasource.common.CmlpRestError;
import org.acumos.datasource.common.ErrorListEnum;
import org.acumos.datasource.common.HelperTool;
import org.acumos.datasource.common.KerberosConfigInfo;
import org.acumos.datasource.common.Utilities;
import org.acumos.datasource.connection.DbUtilitiesV2;
import org.acumos.datasource.exception.CmlpDataSrcException;
import org.acumos.datasource.model.KerberosLogin;
import org.acumos.datasource.schema.DataSourceModelGet;
import org.acumos.datasource.schema.DataSourceMetadata;
import org.acumos.datasource.schema.NameValue;

@Service
public class HdpDataSourceSvcImpl implements HdpDataSourceSvc {

	private static Logger log = LoggerFactory.getLogger(HdpDataSourceSvcImpl.class);

	public HdpDataSourceSvcImpl() {
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
	public String getConnStatusWithKerberos(KerberosLogin objKerberosLogin, String hostName,String readWriteDescriptor) throws IOException, CmlpDataSrcException {
		log.info("Checking kerberos keytab");
		if (!(new File(
				System.getProperty("user.dir") + System.getProperty("file.separator")
						+ objKerberosLogin.getKerberosLoginUser().substring(0,
								objKerberosLogin.getKerberosLoginUser().indexOf("@"))
						+ "." + "kerberos.keytab").exists())) {
			log.info("Kerberos keytab doesn't exist, creating a new one");
			Utilities.createKerberosKeytab(objKerberosLogin, objKerberosLogin.getKerberosKeyTabFileName(), objKerberosLogin.getKerberosConfigFileName());
		}
		log.info("Creating a hadoop configuration with kerberos for " + hostName);
		String result = getConnStatusWithKerberos(hostName, objKerberosLogin.getKerberosLoginUser(), null,readWriteDescriptor);
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
		Utilities.createKerberosKeytab(objKerberosLogin, objKerberosLogin.getKerberosKeyTabFileName(), objKerberosLogin.getKerberosConfigFileName());
		log.info("Created kerberos keytab for principal " + objKerberosLogin.getKerberosLoginUser());
	}

/*	@Override
	public String getConnStatusWithKerberos(String hostName, String kerberosLoginuser, String hdfsFolderName,String readWriteDescriptor)
			throws IOException, CmlpDataSrcException, Exception {
		if (hdfsFolderName == null) {
			return "failed";
		}
		log.info("Starting to establish a test connection to " + hostName + " for hdfs connectivity.");
		Configuration config = HelperTool.getKerberisedConfiguration(hostName,
				kerberosLoginuser);
		log.info("Initializing filesystem with kerberised settings for " + hostName + " for hdfs connectivity.");
		FileSystem fs = FileSystem.get(config);

		
		boolean dirExist=false;
		try{
			if(fs.exists(new Path(hdfsFolderName))){
				log.info("HDFS folder name " + hdfsFolderName+ " exist for readWriteDescriptor "+ readWriteDescriptor);
				FileStatus status=fs.getFileStatus(new Path(hdfsFolderName));
				 if (status.isDirectory()) {
					 dirExist=true;
				 }
			}
		}catch (Exception e) {
			log.info("HDFS folder name " + hdfsFolderName+ " does not exist for readWriteDescriptor "+ readWriteDescriptor);
			e.printStackTrace();
		}
		//removing "&& dirExist" since its not reqquired @am375y
		if((readWriteDescriptor.equalsIgnoreCase("read") ) || (readWriteDescriptor.equalsIgnoreCase("write") && dirExist)){
			log.info("HDFS folder name " + hdfsFolderName+ " exist for readWriteDescriptor"+ readWriteDescriptor);
				return "success";
			}
		if(readWriteDescriptor.equalsIgnoreCase("write") && !dirExist){
			try{
				log.info("HDFS folder name " + hdfsFolderName+ " does not exist for readWriteDescriptor "+ readWriteDescriptor);
				log.info("creating HDFS folder name " + hdfsFolderName);
				fs.mkdirs(new Path(hdfsFolderName));
				return "success";
			}catch (Exception e) {
				e.printStackTrace();
			}
		}
		log.info("HDFS folder name " + hdfsFolderName + "Connection Status failed for readWriteDescriptor "+readWriteDescriptor);
		return "failed";
	}*/
	
	
	@Override
	public String getConnStatusWithKerberos(String hostName, String kerberosLoginuser, String hdfsFolderName,String readWriteDescriptor)
			throws IOException, CmlpDataSrcException {
		
		if (hdfsFolderName == null) {
			return "failed";
		}
		
		try {
			log.info("Starting to establish a test connection to " + hostName + " for hdfs connectivity.");
			Configuration config = HelperTool.getKerberisedConfiguration(hostName,
					kerberosLoginuser);
			
			log.info("Initializing filesystem with kerberised settings for " + hostName + " for hdfs connectivity.");
			FileSystem fs = FileSystem.get(config);
			
			if (readWriteDescriptor.equalsIgnoreCase("read")) {
				if (fs.exists(new Path(hdfsFolderName))) {
					log.info("HDFS folder name " + hdfsFolderName+ " exist for readWriteDescriptor "+ readWriteDescriptor);
					return "success";
				} else {
					log.info("HDFS folder name " + hdfsFolderName+ " does not exist for readWriteDescriptor "+ readWriteDescriptor);
					return "failed";
				}
			}
	
			if (readWriteDescriptor.equalsIgnoreCase("write")) {
				if (fs.exists(new Path(hdfsFolderName))) {
					log.info("HDFS folder name " + hdfsFolderName+ " exist for readWriteDescriptor "+ readWriteDescriptor);
					return "success";
				}
				
				try {
					FileStatus status = fs.getFileStatus(new Path(hdfsFolderName));
					
				} catch (FileNotFoundException fnfe) {
					log.info("HDFS folder name " + hdfsFolderName+ " does not exist. Received FileNotFoundException from HDFS");
					log.info("creating File structure...");
					
					// if the path is a folder, create an empty directory
					// else create an empty file
					
					if (hdfsFolderName.indexOf(".") < 0) {
						log.info("HDFS folder name " + hdfsFolderName+ " does not exist for readWriteDescriptor "+ readWriteDescriptor);
						log.info("Creating HDFS folder name: " + hdfsFolderName);
						boolean isCreated = fs.mkdirs(new Path(hdfsFolderName));
						
						if(isCreated) {
							log.info("Successfully created HDFS folder name: " + hdfsFolderName);
							return "success";
						}
					} else { //File
						//create an empty file
						FSDataOutputStream fsdo = fs.create(new Path(hdfsFolderName));
						if(fsdo != null) {
							fsdo.close();
							log.info("Successfully created HDFS folder name: " + hdfsFolderName);
							return "success";
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		
		log.info("HDFS folder name " + hdfsFolderName + "Connection Status failed for readWriteDescriptor "+readWriteDescriptor);
		return "failed";
	}

	@Override
	public String getConnStatusWithKerberos(KerberosLogin objKerberosLogin, String hostName, String hdfsFolderName,String readWriteDescriptor)
			throws IOException, CmlpDataSrcException {
		/*log.info("Checking kerberos keytab");
		if (!(new File(
				System.getProperty("user.dir") + System.getProperty("file.separator")
						+ objKerberosLogin.getKerberosLoginUser().substring(0,
								objKerberosLogin.getKerberosLoginUser().indexOf("@"))
						+ "." + "kerberos.keytab").exists())) {
			log.info("Kerberos keytab doesn't exist, creating a new one");
			Utilities.createKerberosKeytab(objKerberosLogin);
		}*/
		
		StringBuilder sb1 = new StringBuilder();
		sb1.append(System.getProperty("user.dir")).append(System.getProperty("file.separator"))
		  .append(objKerberosLogin.getKerberosLoginUser().substring(0, objKerberosLogin.getKerberosLoginUser().indexOf("@")))
		  .append(".kerberos.keytab");
	
		if(!Files.exists(Paths.get(sb1.toString()))) {
			log.info("Creating kerberos keytab for hostname " + hostName + " using principal "
					+ objKerberosLogin.getKerberosLoginUser() + " for hdfs connectivity testing.");

			Utilities.createKerberosKeytab(objKerberosLogin, objKerberosLogin.getKerberosKeyTabFileName(), objKerberosLogin.getKerberosConfigFileName());	
		} else {
			try {
				log.info("overwriting kerberos keytab for hostname " + hostName + " using principal "
						+ objKerberosLogin.getKerberosLoginUser() + " for hdfs connectivity testing.");
				Utilities.createKerberosKeytab(objKerberosLogin, objKerberosLogin.getKerberosKeyTabFileName(), objKerberosLogin.getKerberosConfigFileName());
			} catch (Exception e) {
				//ignore
				e.printStackTrace();
			}
		}

		log.info("Testing hive connectivity for hostname " + hostName + " using principal "
				+ objKerberosLogin.getKerberosLoginUser() + " after creating kerberos keytab");
		
		log.info("Creating a hadoop configuration with kerberos for " + hostName);
		return getConnStatusWithKerberos(hostName, objKerberosLogin.getKerberosLoginUser(), hdfsFolderName,readWriteDescriptor);
	}

	@Override
	public InputStream getResults(String user, String authorization, String namespace, String datasourceKey,String hdfsFilename)
			throws CmlpDataSrcException, IOException {

		ArrayList<String> dbDatasourceDetails = dbUtilities.getDataSourceDetails(user, null, null, datasourceKey, null, true, false, authorization);
		//JSONObject objJson = new JSONObject(cassandraDetails.get(0));
		
		DataSourceModelGet dbDataSource = Utilities.getDataSourceModel(dbDatasourceDetails.get(0));
		
		if (dbDataSource.getCategory().equals("hdfs") && dbDataSource.getOwnedBy().equals(user)) {

			Map<String, String> decryptionMap = new HashMap<>();
			//decryptionMap = Utilities.readFromCodeCloud(authorization, datasourceKey);
			decryptionMap = Utilities.readFromMongoCodeCloud(user, datasourceKey);
			
			//if dataReferenceBox is checked, return config file/keytab file contents
			//EE-2557
			//String isDataReference = objJson.optString("isDataReference");
			if (dbDataSource.isDataReference()) {
				return getDataReference(dbDatasourceDetails.get(0), decryptionMap);
			}

			//Restore Kerberos Cache config files if they missed due to POD bounce
			restoreKerberosCacheConfigFiles(decryptionMap);


			log.info("Starting to establish a test connection to " + dbDataSource.getCommonDetails().getServerName()
					+ " for hdfs connectivity.");
			/*Configuration config = HelperTool.getKerberisedConfiguration(
					objJson.getString("serverName"), decrptionMap.get("kerberosLoginUser".toLowerCase()));*/
			
			Configuration config = HelperTool.getKerberisedConfiguration(
					dbDataSource.getCommonDetails().getServerName(), decryptionMap.get("kerberosLoginUser".toLowerCase()));
			
			log.info("Initializing filesystem with kerberised settings for " + dbDataSource.getCommonDetails().getServerName()
					+ " for hdfs connectivity.");
			FileSystem fs = FileSystem.get(config);
			
			log.info("Reading filesystem with kerberised settings for " + dbDataSource.getCommonDetails().getServerName()
					+ " for hdfs connectivity.");
		
			try {
				if (dbDataSource.getHdfsHiveDetails().getHdfsFoldername() != null) {
					FSDataInputStream in;
					
					if(hdfsFilename !=null && !hdfsFilename.isEmpty()) {
						if(dbDataSource.getHdfsHiveDetails().getHdfsFoldername().trim().endsWith("/"))
							in= fs.open(new Path(dbDataSource.getHdfsHiveDetails().getHdfsFoldername().trim() + hdfsFilename));
						else
							in= fs.open(new Path(dbDataSource.getHdfsHiveDetails().getHdfsFoldername().trim() + "/" + hdfsFilename));
					} else {
						in = fs.open(new Path(dbDataSource.getHdfsHiveDetails().getHdfsFoldername().trim()));
					}
					
				if (in.available() > 0) {
					log.info("Reading of " + dbDataSource.getHdfsHiveDetails().getHdfsFoldername()
							+ " succeeded for establishing hdfs connectivity on "
							+ dbDataSource.getCommonDetails().getServerName());
							//return in.getWrappedStream();
							return Utilities.getInputStream(in);
					}
				}

			} catch(Exception e) {
				log.info("Exception occurred Reading File System : " + e.getMessage());
				throw e;
			} finally {
				if(fs != null)
					fs.close();
			}
		}

		//No information
		String[] variables = { "hdfsHiveDetails" };
		
		CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum._0007, variables, null, CmlpApplicationEnum.DATASOURCE);
		
		throw new CmlpDataSrcException(
				"DataSource has invalid Connection parameters. No Sample Results found for the given DatasourceKey.", Status.BAD_REQUEST.getStatusCode(), err);
	}
	
	private InputStream getDataReference(String jsonStr, Map<String, String> decryptionMap) throws CmlpDataSrcException, IOException {
		if (decryptionMap.size() == 0) {
			CmlpRestError err = CmlpErrorList.buildError(new Exception("Unable to retrieve decryption files"), null, CmlpApplicationEnum.DATASOURCE);
			throw new CmlpDataSrcException("Unable to retrieve decryption files",
					Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
		}
		
		JSONObject objJson = new JSONObject(jsonStr);
		
		//remove unwanted fields
		objJson.remove("kerberosConfigFileId");
		objJson.remove("kerberosKeyTabFileId");
		objJson.remove("kerberosLoginUser");
		objJson.remove("_id");
		
		//Add the fields from code cloud
		objJson.put("kerberosconfigfilecontents", Utilities.htmlDecoding(decryptionMap.get("kerberosconfigfilecontents")));
		objJson.put("kerberoskeytabcontent", decryptionMap.get("kerberoskeytabcontent"));
		objJson.put("kerberosloginuser", decryptionMap.get("kerberosloginuser"));
		
		return new ByteArrayInputStream(objJson.toString().getBytes());
	}
	
	
	@Override
	public InputStream getSampleResults(String user, String authorization, String namespace, String datasourceKey,String hdfsFilename)
			throws CmlpDataSrcException, IOException {

		ArrayList<String> dbDatasourceDetails = dbUtilities.getDataSourceDetails(user, null, null, datasourceKey, null, true, false, authorization);
		
		DataSourceModelGet dbDataSource = Utilities.getDataSourceModel(dbDatasourceDetails.get(0));
		
		if (dbDataSource.getCategory().equals("hdfs") && dbDataSource.getOwnedBy().equals(user)) {
			Map<String, String> decryptionMap = new HashMap<>();
			//decryptionMap = Utilities.readFromCodeCloud(authorization, datasourceKey);
			decryptionMap = Utilities.readFromMongoCodeCloud(user, datasourceKey);
			
			//Restore Kerberos Cache config files if they missed due to POD bounce
			restoreKerberosCacheConfigFiles(decryptionMap);
			
			//successfully Restored the files
			log.info("getSampleResults(), Kerberos files were restored successfully...Proceed with check connection");
			
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
			try {
			if (dbDataSource.getHdfsHiveDetails().getHdfsFoldername() != null) {
				FSDataInputStream in;
				
				if(hdfsFilename !=null && !hdfsFilename.isEmpty()) {
					if(dbDataSource.getHdfsHiveDetails().getHdfsFoldername().trim().endsWith("/"))
						in= fs.open(new Path(dbDataSource.getHdfsHiveDetails().getHdfsFoldername().trim() + hdfsFilename));
					else
						in= fs.open(new Path(dbDataSource.getHdfsHiveDetails().getHdfsFoldername().trim() + "/" + hdfsFilename));
				} else {
					in = fs.open(new Path(dbDataSource.getHdfsHiveDetails().getHdfsFoldername().trim()));
				}
				
				if (in != null && in.available() > 0) {
					log.info("Reading of " + dbDataSource.getHdfsHiveDetails().getHdfsFoldername()
							+ " succeeded for establishing hdfs connectivity on "
							+ dbDataSource.getCommonDetails().getServerName());
					int limit = Integer.parseInt(HelperTool.getEnv("datasource_sample_size",
							HelperTool.getComponentPropertyValue("datasource_sample_size"))) * 100;
					byte[] buffer = new byte[limit];
					int bytesRead = in.read(0l, buffer, 0, limit);
					in.close();
					
					return (new  ByteArrayInputStream(buffer));
				}
			}
			} catch(Exception e) {
				log.info("Exception occurred Reading File System : " + e.getMessage());
				throw e;
			} finally {
				if(fs != null)
					fs.close();
			}
		}
		
		//No information
		String[] variables = { "hdfsHiveDetails" };
		
		CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum._0007, variables, null, CmlpApplicationEnum.DATASOURCE);
		
		throw new CmlpDataSrcException(
				"DataSource has invalid Connection parameters. No Sample Results found for the given DatasourceKey.", Status.BAD_REQUEST.getStatusCode(), err);

		
		//String[] variables = {"datasourceKey"};
		
		//CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
		
		//throw new CmlpDataSrcException("No Sample Results found for the given DatasourceKey.",
		//		Status.NOT_FOUND.getStatusCode(), err);
	}

	@Override
	public DataSourceMetadata getMetadata(KerberosLogin objKerberosLogin, String hostName, String hdfsFolderName) throws IOException, CmlpDataSrcException {
		ArrayList<NameValue> metaDataList = new ArrayList<NameValue>();
		DataSourceMetadata metadata = new DataSourceMetadata();
/*		log.info("Checking kerberos keytab");
		if (!(new File(
				System.getProperty("user.dir") + System.getProperty("file.separator")
						+ objKerberosLogin.getKerberosLoginUser().substring(0,
								objKerberosLogin.getKerberosLoginUser().indexOf("@"))
						+ "." + "kerberos.keytab").exists())) {
			log.info("Kerberos keytab doesn't exist, creating a new one");
			Utilities.createKerberosKeytab(objKerberosLogin);
		}*/
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
	
	private void restoreKerberosCacheConfigFiles(Map<String, String> decryptionMap) throws CmlpDataSrcException {
		String krbLoginUser = decryptionMap.get("kerberosLoginUser".toLowerCase());
		
		StringBuilder sb1 = new StringBuilder();
		sb1.append(krbLoginUser.substring(0, krbLoginUser.indexOf("@"))).append(".krb5.conf");
		
		StringBuilder sb2 = new StringBuilder();
		sb2.append(krbLoginUser.substring(0, krbLoginUser.indexOf("@"))).append(".kerberos.keytab");
		
		if(!Utilities.isUserKerberosCacheConfigFilesExists(sb1.toString(), sb2.toString())) {
			//Restore Kerberos Cache files if they are missed due to POD bounce
			log.info("restoreKerberosCacheConfigFiles(), restoring config files from codecloud...");
			
			boolean isWritten = Utilities.writeToKerberosCacheFile(sb1.toString(), decryptionMap.get("KerberosConfigFileContents".toLowerCase()));
			
			if(isWritten) {
				isWritten = Utilities.writeToKerberosCacheFile(sb2.toString(), decryptionMap.get("KerberosKeyTabContent".toLowerCase()));
			}
			
			if(!isWritten) {
				//1. delete config files, if any
				Utilities.deleteUserKerberosCacheConfigFiles(sb1.toString(), sb2.toString());
				
				//2. throw Exception
				CmlpRestError err = CmlpErrorList.buildError(new Exception("Unable to retrieve connection parameters from the codecloud."), null, CmlpApplicationEnum.DATASOURCE);
				throw new CmlpDataSrcException("Unable to retrieve connection parameters from the codecloud.",
						Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
			}
		}
	}

	@Override
	public InputStream getSampleResults(DataSourceModelGet dataSource) throws CmlpDataSrcException, IOException {
		log.info("Starting to establish a test connection to " + dataSource.getCommonDetails().getServerName()
		+ " for hdfs connectivity.");
		Utilities.validateConnectionParameters(dataSource);
		
		String kerberosConfigFileName = dataSource.getHdfsHiveDetails().getKerberosConfigFileId();
		String kerberosKeyTabFileName = dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId();

		KerberosConfigInfo kerberosConfig = Utilities.getKerberosConfigInfo(kerberosConfigFileName, kerberosKeyTabFileName);

		log.info("getSampleResults, Received following Kerberos config info from ConfigFile : "
				+ dataSource.getHdfsHiveDetails().getKerberosConfigFileId());
		log.info(kerberosConfig.toString());

		KerberosLogin objKerberosLogin = new KerberosLogin();
		objKerberosLogin.setKerberosDomainName(kerberosConfig.getDomainName());
		objKerberosLogin.setKerberosKdc(kerberosConfig.getKerberosKdc());
		objKerberosLogin.setKerberosKeyTabContent(kerberosConfig.getKerberosKeyTabContent());
		objKerberosLogin.setKerberosLoginUser(dataSource.getHdfsHiveDetails().getKerberosLoginUser());
		objKerberosLogin.setKerberosPasswordServer(kerberosConfig.getKerberosPasswordServer());
		objKerberosLogin.setKerberosRealms(kerberosConfig.getKerberosRealms());
		objKerberosLogin.setKerbersoAdminServer(kerberosConfig.getKerberosAdminServer());
		objKerberosLogin.setKerberosKeyTabFileName(kerberosKeyTabFileName);
		objKerberosLogin.setKerberosConfigFileName(kerberosConfigFileName);
        
        
        StringBuilder sb1 = new StringBuilder();
	sb1.append(System.getProperty("user.dir")).append(System.getProperty("file.separator"))
	  .append(objKerberosLogin.getKerberosLoginUser().substring(0, objKerberosLogin.getKerberosLoginUser().indexOf("@")))
	  .append(".kerberos.keytab");

	if(!Files.exists(Paths.get(sb1.toString()))) {
		log.info("Creating kerberos keytab for hostname " + dataSource.getCommonDetails().getServerName() + " using principal "
				+ objKerberosLogin.getKerberosLoginUser() + " for hdfs connectivity testing.");

		Utilities.createKerberosKeytab(objKerberosLogin, objKerberosLogin.getKerberosKeyTabFileName(), objKerberosLogin.getKerberosConfigFileName());	
	} else {
		try {
			log.info("overwriting kerberos keytab for hostname " + dataSource.getCommonDetails().getServerName() + " using principal "
					+ objKerberosLogin.getKerberosLoginUser() + " for hdfs connectivity testing.");
			Utilities.createKerberosKeytab(objKerberosLogin, objKerberosLogin.getKerberosKeyTabFileName(), objKerberosLogin.getKerberosConfigFileName());
		} catch (Exception e) {
			//ignore
			e.printStackTrace();
		}
	}
		
		
		Configuration config = HelperTool.getKerberisedConfiguration(
				dataSource.getCommonDetails().getServerName(), dataSource.getHdfsHiveDetails().getKerberosLoginUser());

		log.info("Initializing filesystem with kerberised settings for " + dataSource.getCommonDetails().getServerName()
		+ " for hdfs connectivity.");
		FileSystem fs = FileSystem.get(config);
		FSDataInputStream in = fs.open(new Path(dataSource.getHdfsHiveDetails().getHdfsFoldername()));
		log.info("Reading filesystem with kerberised settings for " + dataSource.getCommonDetails().getServerName()
		+ " for hdfs connectivity.");
		
		byte[] buffer = null;
				
		if (in.available() > 0) {
			log.info("Reading of " + dataSource.getHdfsHiveDetails().getHdfsFoldername()
			+ " succeeded for establishing hdfs connectivity on " + dataSource.getCommonDetails().getServerName());
			int limit = Integer.parseInt(HelperTool.getEnv("datasource_sample_size",
					HelperTool.getComponentPropertyValue("datasource_sample_size"))) * 100;
			buffer = new byte[limit];
			@SuppressWarnings("unused")
			int bytesRead = in.read(0l, buffer, 0, limit);
			
			// return in.getWrappedStream();s
		} 
		
		Utilities.deleteUserKerberosConfigFiles(dataSource.getHdfsHiveDetails().getKerberosConfigFileId(),
				dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId());
		return (new ByteArrayInputStream(buffer));
	}
	@Override
	public boolean writebackPrediction(String user, String authorization, DataSourceModelGet dataSource, String data,String hdfsFilename) throws IOException, CmlpDataSrcException {
		log.info("writebackPrediction :  starting");

		boolean status = false;
		Map<String, String> decryptionMap = new HashMap<>();
		//decryptionMap = Utilities.readFromCodeCloud(authorization, dataSource.getDatasourceKey());
		decryptionMap = Utilities.readFromMongoCodeCloud(user, dataSource.getDatasourceKey());
		
		Configuration config = HelperTool.getKerberisedConfiguration(
				dataSource.getCommonDetails().getServerName(),
				decryptionMap.get("kerberosLoginUser".toLowerCase()));

		try {
			
			FileSystem fs = FileSystem.get(config);

			if (dataSource.getHdfsHiveDetails().getHdfsFoldername() != null || hdfsFilename!=null && dataSource.getReadWriteDescriptor().equalsIgnoreCase("write")) {
				log.info("writebackPrediction :  create hdfs file "+dataSource.getHdfsHiveDetails().getHdfsFoldername()+"/"+hdfsFilename);
				FSDataOutputStream out = fs.create(new Path(dataSource.getHdfsHiveDetails().getHdfsFoldername()+"/"+hdfsFilename));
				log.info("writebackPrediction :  write data-->"+data+" to hdfs file "+dataSource.getHdfsHiveDetails().getHdfsFoldername());
				out.writeBytes(data);
				out.close();
				status = true;
			}
			
		} catch (Exception e) {
			String[] variables = { e.getMessage()};
			CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
			throw new CmlpDataSrcException("Invalid insert query parameter",
					Status.BAD_REQUEST.getStatusCode(), err);
		}
		return status;
	}
	
	
	private String simpleKerberosTest(String hostName, String kerberosLoginuser, String hdfsFolderName,String readWriteDescriptor)
			throws IOException, CmlpDataSrcException {
		
		Configuration conf = new Configuration(); 
		conf.set("fs.defaultFS", "hdfs://zlp25101.vci.att.com:8020"); 
		conf.set("hadoop.security.authentication", "Kerberos"); 
		conf.set("dfs.datanode.kerberos.principal", kerberosLoginuser);

		String krbKeytabFilename = System.getProperty("user.dir") + System.getProperty("file.separator") + "m09286.kerberos.keytab";
		String krb5ConfFilename = System.getProperty("user.dir") + System.getProperty("file.separator") + "m09286.krb5.conf";
		
		System.setProperty("java.security.krb5.conf", krb5ConfFilename);
		System.setProperty("javax.security.auth.useSubjectCredsOnly", "true");
		System.setProperty("dfs.datanode.kerberos.principal", kerberosLoginuser);
				
		UserGroupInformation.setConfiguration(conf); 
		UserGroupInformation.loginUserFromKeytab("m09286@PROD.HADOOP.CMLP.ATT.COM", krbKeytabFilename);

		FileSystem fs = FileSystem.get(conf); 
		
		FileStatus[] fsStatus = fs.listStatus(new Path("/")); 
		
		for(int i = 0; i < fsStatus.length; i++) { 
			System.out.println(fsStatus[i].getPath().toString()); 
		}
		
		return null;
		
	}

}
