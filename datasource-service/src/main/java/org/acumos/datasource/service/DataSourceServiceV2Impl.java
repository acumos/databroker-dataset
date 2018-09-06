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
import java.lang.invoke.MethodHandles;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.acumos.datasource.connection.DbUtilitiesV2;
import org.acumos.datasource.exception.DataSrcException;
import org.acumos.datasource.model.CassandraConnectionModel;
import org.acumos.datasource.model.FileConnectionModel;
import org.acumos.datasource.model.JdbcConnectionModel;
import org.acumos.datasource.model.JobSubmissionYarn;
import org.acumos.datasource.model.KerberosLogin;
import org.acumos.datasource.model.MongoDbConnectionModel;
import org.acumos.datasource.model.MysqlConnectorModel;
import org.acumos.datasource.model.SparkYarnTestModel;
import org.acumos.datasource.schema.DataSourceModelGet;
import org.acumos.datasource.schema.DataSourceModelPost;
import org.acumos.datasource.schema.DataSourceModelPut;
import org.acumos.datasource.utils.ApplicationUtilities;
import org.json.JSONObject;
import org.acumos.datasource.common.CmlpApplicationEnum;
import org.acumos.datasource.common.DataSrcErrorList;
import org.acumos.datasource.common.DataSrcRestError;
import org.acumos.datasource.common.ErrorListEnum;
import org.acumos.datasource.common.HelperTool;
import org.acumos.datasource.common.KerberosConfigInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.ws.rs.core.Response.Status;

@Service
public class DataSourceServiceV2Impl implements DataSourceServiceV2 {
	
	private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	
	@Autowired
	private DbUtilitiesV2 dbUtilities;
	
	@Autowired
	private CassandraDataSourceSvcImpl cassandraSvc;

	@Autowired
	private HdpDataSourceSvcImpl hdpSvc;

	@Autowired
	private HiveDataSourceSvcImpl hiveSvc;

	@Autowired
	private MongoDataSourceSvcImpl mongoSvc;

	@Autowired
	private MySqlDataSourceSvcImpl mySqlSvc;

	@Autowired
	private SparkStandaloneSvcImpl sparkSASvc;

	@Autowired
	private SparkYarnSvcImpl sparkYarnSvc;

	@Autowired
	private FileDataSourceSvcImpl fileSvc;

	@Autowired
	private JdbcDataSourceSvcImpl jdbcSvc;

	@Autowired
	private HdpBatchDataSourceSvcImpl hdpBatchSvc;

	@Autowired
	private HiveBatchDataSourceSvcImpl hiveBatchSvc;

	@Override
	public List<String> getDataSourcesList(String user, String authorization, String namespace, String category,
			String dataSourcekey, String textSearch) throws DataSrcException, IOException {

		List<String> results = null;
		
		try {
			results = dbUtilities.getDataSourceDetails(user, namespace, category, dataSourcekey, textSearch, false, true, authorization);
		} catch (Exception exc) {
			log.info("getDataSourcesList, Unknown Exception : " + exc.getMessage());
			DataSrcRestError err = DataSrcErrorList.buildError(exc, null, CmlpApplicationEnum.DATASOURCE);
			
			throw new DataSrcException("Exception occurred during GET.",
					Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
		}
		
		return results;
	}

	@Override
	public boolean deleteDataSourceDetail(String user, String datasourceKey) throws DataSrcException, IOException {
		
		log.info("deleteDataSourceDetail()::loggd in user: " + user);
		log.info("deleteDataSourceDetail():: datasourceKey: " + datasourceKey);
		
		try {
			//validate and throw Exceptions
			validateDataSource(user, datasourceKey);
			
			String deleteType = "";
			deleteType = HelperTool.getEnv("dataSource_delete_type",
					HelperTool.getComponentPropertyValue("dataSource_delete_type"));
			deleteType = deleteType != null ? deleteType : "softdelete";
			if (deleteType.equals("softdelete"))
				return dbUtilities.softDeleteDataSource(user, datasourceKey);
			else
				return dbUtilities.deleteDataSource(user, datasourceKey);
			
		} catch (DataSrcException cmlpExc) {
			throw cmlpExc;
		} catch (Exception exc) {
			log.info("deleteDataSourceDetail, Unknown Exception : " + exc.getMessage());
			DataSrcRestError err = DataSrcErrorList.buildError(exc, null, CmlpApplicationEnum.DATASOURCE);
			
			throw new DataSrcException("Exception occurred during DELETE.",
					Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
		}
	}

	@Override
	public String saveDataSourceDetail(String user, String authorization, String codeCloudAuthorization,
			DataSourceModelPost dataSourcePost) throws DataSrcException, IOException, SQLException, ClassNotFoundException {
		log.info("saveDataSourceDetail, logged in user: " + user);
		
		DataSourceModelGet dataSource = new DataSourceModelGet(dataSourcePost);
		dataSource.setOwnedBy(user);
		
		dataSource = ApplicationUtilities.validateInputRequest(dataSource, codeCloudAuthorization, "create");
		
		
		dataSource.setOwnedBy(user);
		dataSource.setActive(true);// default status is true;
		
		String serverName = "";
		if(dataSource.getCommonDetails() != null && dataSource.getCommonDetails().getServerName() != null)
			serverName = dataSource.getCommonDetails().getServerName();
		dataSource.setDatasourceKey(ApplicationUtilities.getName(dataSource.getNamespace(), dataSource.getCategory(), serverName, user));
		
		log.info("saveDataSourceDetail, key for new datasource detail: " + dataSource.getDatasourceKey());
		log.info("saveDataSourceDetail, category for new datasource detail: " + dataSource.getCategory());
		log.info("saveDataSourceDetail, name for new datasource detail: " + dataSource.getDatasourceName());

		// check null values in the parameters that are required for connection
		String resultsForConnection = checkDataSourcesDetails(user, authorization, codeCloudAuthorization, dataSource, null, "create");
		if (resultsForConnection.equals("success")) {

			dbUtilities.insertDataSourceDetails(dataSource);

			log.info("saveDataSourceDetail, Successfully created new datasource : " + dataSource.getDatasourceKey());

			// clean up process
			if (dataSource.getHdfsHiveDetails() != null && dataSource.getHdfsHiveDetails().getKerberosConfigFileId() != null) {
				ApplicationUtilities.deleteUserKerberosConfigFiles(dataSource.getHdfsHiveDetails().getKerberosConfigFileId(),
						dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId());
			}

			return dataSource.getDatasourceKey();

		} else {
			// clean up process
			if (dataSource.getHdfsHiveDetails() != null && dataSource.getHdfsHiveDetails().getKerberosConfigFileId() != null) {
				ApplicationUtilities.deleteUserKerberosConfigFiles(dataSource.getHdfsHiveDetails().getKerberosConfigFileId(),
						dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId());
			}

			log.info("saveDataSourceDetail, Failed to create a new datasource : " + dataSource.getDatasourceKey());
			ApplicationUtilities.raiseConnectionFailedException(dataSource.getCategory());
		}
		
		return null;
	}

	@Override
	public boolean updateDataSourceDetail(String user, String authorization, String codeCloudAuthorization,
			String datasourceKey, DataSourceModelPut dataSourcePut) throws DataSrcException, IOException {
		
		log.info("updateDataSourceDetail, user: " + user);
		log.info("updateDataSourceDetail, datasourceKey: " + datasourceKey);
		
		try {
			//validate and throw Exceptions
			validateDataSource(user, datasourceKey);

			DataSourceModelGet dataSource = new DataSourceModelGet(dataSourcePut);
			dataSource.setDatasourceKey(datasourceKey);
			dataSource.setOwnedBy(user);
	
			log.info("updateDataSourceDetail, logged in user: " + user);
	
			//Validate input parameters
			dataSource = ApplicationUtilities.validateInputRequest(dataSource, codeCloudAuthorization, "update");
	
			log.info("updateDataSourceDetail, key for new datasource detail: " + dataSource.getDatasourceKey());
			log.info("updateDataSourceDetail, category for new datasource detail: " + dataSource.getCategory());
	
			// check null values in the parameters that are required for connection
			if (checkDataSourcesDetails(user, authorization, codeCloudAuthorization, dataSource, datasourceKey, "update")
					.equals("success")) {
	
				// clean up process
				if (dataSource.getHdfsHiveDetails() != null && dataSource.getHdfsHiveDetails().getKerberosConfigFileId() != null) {
					ApplicationUtilities.deleteUserKerberosConfigFiles(dataSource.getHdfsHiveDetails().getKerberosConfigFileId(),
							dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId());
				}
				
				return dbUtilities.updateDataSource(user, datasourceKey, dataSource);
				
			} else {
				// clean up process
				if (dataSource.getHdfsHiveDetails() != null && dataSource.getHdfsHiveDetails().getKerberosConfigFileId() != null) {
					ApplicationUtilities.deleteUserKerberosConfigFiles(dataSource.getHdfsHiveDetails().getKerberosConfigFileId(),
							dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId());
				}
				
				ApplicationUtilities.raiseConnectionFailedException(dataSource.getCategory());
			}
			
			return false;
			
		} catch (DataSrcException cmlpExc) {
			throw cmlpExc;
		} catch (Exception exc) {
			log.info("updateDataSourceDetail, Unknown Exception : " + exc.getMessage());
			DataSrcRestError err = DataSrcErrorList.buildError(exc, null, CmlpApplicationEnum.DATASOURCE);
			
			throw new DataSrcException("Exception occurred during PUT.",
					Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
		}
	}

	@Override
	public InputStream getDataSourceContents(String user, String authorization, String datasourceKey,
			String hdfsFilename) throws DataSrcException, IOException, SQLException, ClassNotFoundException {
		log.info("getDataSourcesContents, user: " + user);
		log.info("getDataSourcesContents, datasourceKey: " + datasourceKey);
		
		//validate and throw Exceptions
		validateDataSource(user, datasourceKey);
		
		ArrayList<String> dbDatasourceDetails = dbUtilities.getDataSourceDetails(user, null, null, datasourceKey, null, true, false, authorization);
		
		String namespace = null;
		
		JSONObject objJson = new JSONObject(dbDatasourceDetails.get(0));
		
		// calling different service method based on category of the database
		if (objJson.getString("ownedBy").equals(user)) {
			if (objJson.getString("category").equals("cassandra")) {
				return cassandraSvc.getResults(user, authorization, namespace, datasourceKey);
			} else if (objJson.getString("category").equals("mysql")) {
				return mySqlSvc.getResults(user, authorization, namespace, datasourceKey);
			} else if (objJson.getString("category").equals("mongo")) {
				return mongoSvc.getResults(user, authorization, namespace, datasourceKey);
			} else if (objJson.getString("category").equals("hive")) {
				return hiveSvc.getResults(user, authorization, namespace, datasourceKey);
			} else if (objJson.getString("category").equals("hdfs")) {
				return hdpSvc.getResults(user, authorization, namespace, datasourceKey,hdfsFilename);
			} else if (objJson.getString("category").equals("file")) {
				return fileSvc.getResults(user, authorization, namespace, datasourceKey);
			} else if (objJson.getString("category").equals("jdbc")) {
				return jdbcSvc.getResults(user, authorization, namespace, datasourceKey);
			} else if (objJson.getString("category").equals("hive batch")) {
				return hiveBatchSvc.getResults(user, authorization, namespace, datasourceKey, objJson.getString("batchSize"));
			} else if (objJson.getString("category").equals("hdfs batch")) {
				return hdpBatchSvc.getResults(user, authorization, namespace, datasourceKey, objJson.getString("batchSize"));
			}
		} else {
			String[] variables = {"Authorization"};
			DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._1003, variables, null, CmlpApplicationEnum.DATASOURCE);
			throw new DataSrcException(
					"please check dataset key provided and user permission for this operation", Status.UNAUTHORIZED.getStatusCode(), err);
		}
		
		//won't reach up to this point as datasource in db always have a valid category
		return null;
	}

	@Override
	public String checkDataSourcesDetails(String user, String authorization, String codeCloudAuthorization,
			DataSourceModelGet dataSource, String datasourceKey, String mode) throws DataSrcException, IOException, SQLException, ClassNotFoundException {
		
		log.info("checkDataSourcesDetails, user: " + user);
		log.info("checkDataSourcesDetails, datasourceKey: " + datasourceKey);
		
		//validate and throw Exceptions
		if (!"create".equalsIgnoreCase(mode)) {
			validateDataSource(user, datasourceKey);
		}
		
		String connectionStatus = "failed";
		String enAssociation = "";
		boolean isKerberosInfoUpdated = false;
		boolean isCreate = false;
		boolean isUpdate = false;

		
		if(dataSource == null) {
		
			List<String> dbObjects = dbUtilities.getDataSourceDetails(user, null, null, datasourceKey, null, false, true, authorization);
			
			if(dbObjects != null && dbObjects.size() > 0) {
				dataSource = ApplicationUtilities.getDataSourceModel(dbObjects.get(0));
			}
		}

		//validate input for the required connection parameters
		ApplicationUtilities.validateConnectionParameters(dataSource);
		
		if ("create".equalsIgnoreCase(mode))
			isCreate = true;
		else if ("update".equalsIgnoreCase(mode))
			isUpdate = true;
		
		// Check for Kerberos config during update
		if (isUpdate) {
			if (dataSource.getCategory().equals("hive")
					|| dataSource.getCategory().equals("hive batch") || dataSource.getCategory().equals("hdfs batch")
					|| dataSource.getCategory().equals("Spark on Yarn") || dataSource.getCategory().equals("hdfs")) {
				log.info("checkDataSourcesDetails(), Mode is Update and Category is " + dataSource.getCategory());
				
				log.info("checkDataSourcesDetails(), Checking for Kerberos connection parameters");

				// Check the KerberosConfig filenames and restore if required
				log.info("checkDataSourcesDetails(), Fetching datasource from db");
				List<String> dbDatasourceDetails = dbUtilities.getDataSourceDetails(user, null, null,
						dataSource.getDatasourceKey(), null, false, true, authorization);

				log.info("checkDataSourcesDetails(), Building datasourcemodel...");
				DataSourceModelGet dbDataSource = ApplicationUtilities.getDataSourceModel(dbDatasourceDetails.get(0));

				log.info("checkDataSourcesDetails(), Request kerberosConfigFileName : "
						+ dataSource.getHdfsHiveDetails().getKerberosConfigFileId());
				log.info("checkDataSourcesDetails(), Request kerberosKeyTabFileName : "
						+ dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId());
				log.info("checkDataSourcesDetails(), DB kerberosConfigFileName : "
						+ dbDataSource.getHdfsHiveDetails().getKerberosConfigFileId());
				log.info("checkDataSourcesDetails(), DB kerberosKeyTabFileName : "
						+ dbDataSource.getHdfsHiveDetails().getKerberosKeyTabFileId());

				if ((dataSource.getHdfsHiveDetails().getKerberosConfigFileId() != null
						&& (!dataSource.getHdfsHiveDetails().getKerberosConfigFileId().equals(dbDataSource.getHdfsHiveDetails().getKerberosConfigFileId())))
						|| (dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId() != null && (!dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId()
								.equals(dbDataSource.getHdfsHiveDetails().getKerberosKeyTabFileId())))) {
					isKerberosInfoUpdated = true;
				}

				if (!isKerberosInfoUpdated) {
					log.info(
							"checkDataSourcesDetails(), Kerberos files were NOT updated. Need to restore config files from codecloud...");
					dataSource.getHdfsHiveDetails().setKerberosConfigFileId(
							ApplicationUtilities.getKerberosFileName(user, dataSource.getHdfsHiveDetails().getKerberosConfigFileId()) + ".krb5.conf");
					dataSource.getHdfsHiveDetails().setKerberosKeyTabFileId(
							ApplicationUtilities.getKerberosFileName(user, dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId()) + ".keytab");

					// decryption of properties
					//Map<String, String> decryptionMap = Utilities.readFromCodeCloud(authorization,
					//		dataSource.getDatasourceKey());
					
					Map<String, String> decryptionMap = ApplicationUtilities.readFromMongoCodeCloud(user, dataSource.getDatasourceKey());

					boolean isWritten = ApplicationUtilities.writeToKerberosFile(dataSource.getHdfsHiveDetails().getKerberosConfigFileId(),
							decryptionMap.get("KerberosConfigFileContents".toLowerCase()));

					if (isWritten) {
						isWritten = ApplicationUtilities.writeToKerberosFile(dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId(),
								decryptionMap.get("KerberosKeyTabContent".toLowerCase()));
					}

					if (!isWritten) {
						// 1. delete config files, if any
						ApplicationUtilities.deleteUserKerberosConfigFiles(dataSource.getHdfsHiveDetails().getKerberosConfigFileId(),
								dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId());

						// 2. throw Exception
						ApplicationUtilities.raiseConnectionFailedException(dataSource.getCategory());
					}

					// successfully Restored the files
					log.info(
							"checkDataSourcesDetails(), Kerberos files were restored successfully...Proceed with check connection");

				} else {
					log.info(
							"checkDataSourcesDetails(), Kerberos files were updated. No need to restore config files...");
					log.info("checkDataSourcesDetails(), Proceed with check connection...");
				}
			}
		}

		// checking all necessary parameters before initializing and sending
		// details for connection status check
		if (dataSource.getCategory().equals("cassandra")) {
			
			CassandraConnectionModel objCassandraConnectionModel;
			
			log.info("checkDataSourcesDetails, connection test for cassandra datasource will be initiated");
			objCassandraConnectionModel = new CassandraConnectionModel();
			objCassandraConnectionModel.setNode(dataSource.getCommonDetails().getServerName());
			objCassandraConnectionModel.setPassword(dataSource.getDbDetails().getDbServerPassword());
			objCassandraConnectionModel.setPort(dataSource.getCommonDetails().getPortNumber());
			objCassandraConnectionModel.setUserName(dataSource.getDbDetails().getDbServerUsername());
			objCassandraConnectionModel.setKeyspace(dataSource.getDbDetails().getDatabaseName());
			
			try {
				connectionStatus = cassandraSvc.getConnectionStatus(objCassandraConnectionModel, dataSource.getDbDetails().getDbQuery());
			} catch (Exception e) {
				log.info("checkDataSourcesDetails, Exception occurred while checking connection : " + e.getMessage());
			}

			log.info(
					"checkDataSourcesDetails, connection test for cassandra datasource has been completed and status is: "
							+ connectionStatus);

			if (connectionStatus.equals("success") && (isCreate || isUpdate)) {
				if(Boolean.getBoolean(HelperTool.getEnv("is_spring_configserver_on", "false"))) {
					Map<String, String> encryptionMap = new HashMap<>();
					encryptionMap.put("DbServerUsername",
							ApplicationUtilities.getEncrypt(authorization, dataSource.getDbDetails().getDbServerUsername()));
					encryptionMap.put("DbServerPassword",
							ApplicationUtilities.getEncrypt(authorization, dataSource.getDbDetails().getDbServerPassword()));
					enAssociation = ApplicationUtilities.doEncryptDecryptCommit(authorization, codeCloudAuthorization,
							dataSource.getDatasourceKey(), encryptionMap, mode);
					dataSource.getDbDetails().setDbServerUsername(enAssociation);
					dataSource.getDbDetails().setDbServerPassword(enAssociation);
					dataSource.setMetaData(objCassandraConnectionModel.getMetaData());
				} else {
					Map<String, String> encryptionMap = new HashMap<>();
					encryptionMap.put("DbServerUsername", dataSource.getDbDetails().getDbServerUsername());
					encryptionMap.put("DbServerPassword", dataSource.getDbDetails().getDbServerPassword());
					
					//enAssociation = Utilities.writeToCodeCloud(authorization, codeCloudAuthorization, dataSource.getDatasourceKey(), encryptionMap);
					enAssociation = ApplicationUtilities.writeToCodeCloudInMongo(user, dataSource.getDatasourceKey(), encryptionMap, mode);
					
					dataSource.getDbDetails().setDbServerUsername(enAssociation);
					dataSource.getDbDetails().setDbServerPassword(enAssociation);
					dataSource.setMetaData(objCassandraConnectionModel.getMetaData());
				}
				
				
			}
		}
		
		// check null values in the parameters that are required for connecting
		// to hive host
		else if (dataSource.getCategory().equals("hive")) {

			KerberosConfigInfo kerberosConfig = null;

			log.info("checkDataSourcesDetails, connection test for hive datasource will be initiated");
			if(mode.equalsIgnoreCase("validateConnection")) {
				InputStream in = hiveSvc.getSampleResults(user, authorization, null, datasourceKey);
				
				if (in != null && in.available() > 0) {
					return "success";
				} else {
					return "failed";
				}
			}

			String kerberosConfigFileName = dataSource.getHdfsHiveDetails().getKerberosConfigFileId();
			String kerberosKeyTabFileName = dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId();

			kerberosConfig = ApplicationUtilities.getKerberosConfigInfo(kerberosConfigFileName, kerberosKeyTabFileName);

			log.info("checkDataSourcesDetails, Received following Kerberos config info from ConfigFile : "
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

			try {
				connectionStatus = hiveSvc.getConnectionStatusWithKerberos(objKerberosLogin, dataSource.getCommonDetails().getServerName(),
						String.valueOf(dataSource.getCommonDetails().getPortNumber()), dataSource.getDbDetails().getDbQuery());
			} catch (Exception e) {
				log.info("checkDataSourcesDetails, Exception occurred while checking connection : " + e.getMessage());
			}
			if (connectionStatus.equals("success")) {
				dataSource.setMetaData(objKerberosLogin.getMetaData());
			}

			if (connectionStatus.equals("success") && (isCreate || (isUpdate && isKerberosInfoUpdated))) {

				if(Boolean.getBoolean(HelperTool.getEnv("is_spring_configserver_on", "false"))) {
					Map<String, String> encryptionMap = new HashMap<>();
					encryptionMap.put("KerberosLoginUser",
							ApplicationUtilities.getEncrypt(authorization, dataSource.getHdfsHiveDetails().getKerberosLoginUser()));
					encryptionMap.put("KerberosKeyTabContent",
							ApplicationUtilities.getEncrypt(authorization, kerberosConfig.getKerberosKeyTabContent()));
					encryptionMap.put("KerberosConfigFileContents",
							ApplicationUtilities.getEncrypt(authorization, kerberosConfig.getConfigFileContents()));
					log.info("checkDataSourcesDetails(), Sending encrypted kerberos(HIVE) information to ConfigMgmt");
					enAssociation = ApplicationUtilities.doEncryptDecryptCommit(authorization, codeCloudAuthorization,
							dataSource.getDatasourceKey(), encryptionMap, mode);
					log.info("checkDataSourcesDetails(), Successfully saved kereberos(HIVE) information in ConfigMgmt : "
							+ enAssociation);
					dataSource.getHdfsHiveDetails().setKerberosLoginUser(enAssociation);
				} else {
					Map<String, String> encryptionMap = new HashMap<>();
					encryptionMap.put("KerberosLoginUser", dataSource.getHdfsHiveDetails().getKerberosLoginUser());
					encryptionMap.put("KerberosKeyTabContent", kerberosConfig.getKerberosKeyTabContent());
					encryptionMap.put("KerberosConfigFileContents", kerberosConfig.getConfigFileContents());
					
					//enAssociation = Utilities.writeToCodeCloud(authorization, codeCloudAuthorization, dataSource.getDatasourceKey(), encryptionMap);
					enAssociation = ApplicationUtilities.writeToCodeCloudInMongo(user, dataSource.getDatasourceKey(), encryptionMap, mode);
					
					dataSource.getHdfsHiveDetails().setKerberosLoginUser(enAssociation);
				}
			}
		}
		// check null values in the parameters that are required for connecting
		// to mongo host
		else if (dataSource.getCategory().equals("mongo")) {
			log.info("checkDataSourcesDetails, connection test for mongo datasource will be initiated");
			MongoDbConnectionModel objMongoDbConnectionModel = new MongoDbConnectionModel();
			objMongoDbConnectionModel.setDbName(dataSource.getDbDetails().getDatabaseName());
			objMongoDbConnectionModel.setHostname(dataSource.getCommonDetails().getServerName());
			objMongoDbConnectionModel.setPort(dataSource.getCommonDetails().getPortNumber());
			objMongoDbConnectionModel.setCollectionName(dataSource.getDbDetails().getDbCollectionName());

			if (dataSource.getDbDetails().getDbServerPassword() != null 
					&& dataSource.getDbDetails().getDbServerUsername() != null ) {
				objMongoDbConnectionModel.setPassword(dataSource.getDbDetails().getDbServerPassword());
				objMongoDbConnectionModel.setUsername(dataSource.getDbDetails().getDbServerUsername());
			}

			try {
				connectionStatus = mongoSvc.getConnectionStatus_2_10(objMongoDbConnectionModel, dataSource.getDbDetails().getDbQuery());
			} catch (Exception e) {
				log.info("checkDataSourcesDetails, Exception occurred while checking connection : " + e.getMessage());
			}

			if (connectionStatus.equals("success") && (isCreate || isUpdate)
					&& dataSource.getDbDetails().getDbServerPassword() != null 
					&& dataSource.getDbDetails().getDbServerUsername() != null ) {
				if(Boolean.getBoolean(HelperTool.getEnv("is_spring_configserver_on", "false"))) {
					dataSource.setMetaData(objMongoDbConnectionModel.getMetadata());
					Map<String, String> encryptionMap = new HashMap<>();
					encryptionMap.put("DbServerUsername",
							ApplicationUtilities.getEncrypt(authorization, dataSource.getDbDetails().getDbServerUsername()));
					encryptionMap.put("DbServerPassword",
							ApplicationUtilities.getEncrypt(authorization, dataSource.getDbDetails().getDbServerPassword()));
					log.info("checkDataSourcesDetails(), Sending encrypted Mongo information to ConfigMgmt");
					enAssociation = ApplicationUtilities.doEncryptDecryptCommit(authorization, codeCloudAuthorization,
							dataSource.getDatasourceKey(), encryptionMap, mode);
					log.info("checkDataSourcesDetails(), Successfully saved Mongo information in ConfigMgmt : "
							+ enAssociation);
					dataSource.getDbDetails().setDbServerUsername(enAssociation);
					dataSource.getDbDetails().setDbServerPassword(enAssociation);
				} else {
					Map<String, String> encryptionMap = new HashMap<>();
					encryptionMap.put("DbServerUsername", dataSource.getDbDetails().getDbServerUsername());
					encryptionMap.put("DbServerPassword", dataSource.getDbDetails().getDbServerPassword());
					
					//enAssociation = Utilities.writeToCodeCloud(authorization, codeCloudAuthorization, dataSource.getDatasourceKey(), encryptionMap);
					enAssociation = ApplicationUtilities.writeToCodeCloudInMongo(user, dataSource.getDatasourceKey(), encryptionMap, mode);
					
					dataSource.getDbDetails().setDbServerUsername(enAssociation);
					dataSource.getDbDetails().setDbServerPassword(enAssociation);
					dataSource.setMetaData(objMongoDbConnectionModel.getMetadata());
				}
			}
		}
		// check null values in the parameters that are required for connecting
		// to mysql host
		else if (dataSource.getCategory().equals("mysql")) {
			log.info("checkDataSourcesDetails, connection test for mysql datasource will be initiated");
			MysqlConnectorModel objMysqlConnectorModel = new MysqlConnectorModel();
			objMysqlConnectorModel.setHostname(dataSource.getCommonDetails().getServerName());
			objMysqlConnectorModel.setPassword(dataSource.getDbDetails().getDbServerPassword());
			objMysqlConnectorModel.setPort(String.valueOf(dataSource.getCommonDetails().getPortNumber()));
			objMysqlConnectorModel.setUserName(dataSource.getDbDetails().getDbServerUsername());
			objMysqlConnectorModel.setDbName(dataSource.getDbDetails().getDatabaseName());

			try {
				connectionStatus = mySqlSvc.getConnectionStatus(objMysqlConnectorModel, dataSource.getDbDetails().getDbQuery());
			} catch (Exception e) {
				log.info("checkDataSourcesDetails, Exception occurred while checking connection : " + e.getMessage());
			}

			if (connectionStatus.equals("success") && (isCreate || isUpdate)) {
				if(Boolean.getBoolean(HelperTool.getEnv("is_spring_configserver_on", "false"))) {
					dataSource.setMetaData(objMysqlConnectorModel.getMetaData());
					Map<String, String> encryptionMap = new HashMap<>();
					encryptionMap.put("DbServerUsername",
							ApplicationUtilities.getEncrypt(authorization, dataSource.getDbDetails().getDbServerUsername()));
					encryptionMap.put("DbServerPassword",
							ApplicationUtilities.getEncrypt(authorization, dataSource.getDbDetails().getDbServerPassword()));
					log.info("checkDataSourcesDetails(), Sending encrypted MySQL information to ConfigMgmt");
					enAssociation = ApplicationUtilities.doEncryptDecryptCommit(authorization, codeCloudAuthorization,
							dataSource.getDatasourceKey(), encryptionMap, mode);
					log.info("checkDataSourcesDetails(), Successfully saved MySQL information in ConfigMgmt : "
							+ enAssociation);
					dataSource.getDbDetails().setDbServerUsername(enAssociation);
					dataSource.getDbDetails().setDbServerPassword(enAssociation);
				} else {
					Map<String, String> encryptionMap = new HashMap<>();
					encryptionMap.put("DbServerUsername", dataSource.getDbDetails().getDbServerUsername());
					encryptionMap.put("DbServerPassword", dataSource.getDbDetails().getDbServerPassword());
					
					//enAssociation = Utilities.writeToCodeCloud(authorization, codeCloudAuthorization, dataSource.getDatasourceKey(), encryptionMap);
					enAssociation = ApplicationUtilities.writeToCodeCloudInMongo(user, dataSource.getDatasourceKey(), encryptionMap, mode);
					
					dataSource.getDbDetails().setDbServerUsername(enAssociation);
					dataSource.getDbDetails().setDbServerPassword(enAssociation);
					dataSource.setMetaData(objMysqlConnectorModel.getMetaData());
				}
			}
		}
		// check null values in the parameters that are required for connecting
		// to spark standalone host
		else if (dataSource.getCategory().equals("Spark Standalone")) {
			log.info("checkDataSourcesDetails, connection test for Spark Standalone datasource will be initiated");

			try {
				connectionStatus = sparkSASvc.getConnectionStatus(dataSource.getCommonDetails().getServerName(),
						String.valueOf(dataSource.getCommonDetails().getPortNumber()));
			} catch (Exception e) {
				log.info("checkDataSourcesDetails, Exception occurred while checking connection : " + e.getMessage());
			}
		}
		// check null values in the parameters that are required for connecting
		// to spark on yarn host
		else if (dataSource.getCategory().equals("Spark on Yarn")) {
			log.info("checkDataSourcesDetails, connection test for Spark on Yarn datasource will be initiated");

			String kerberosConfigFileName = dataSource.getHdfsHiveDetails().getKerberosConfigFileId();
			String kerberosKeyTabFileName = dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId();

			KerberosConfigInfo kerberosConfig = ApplicationUtilities.getKerberosConfigInfo(kerberosConfigFileName,
					kerberosKeyTabFileName);

			log.info("checkDataSourcesDetails, connection test for Spark on Yarn datasource will be initiated");
			KerberosLogin objKerberosLogin = new KerberosLogin();
			objKerberosLogin.setKerberosDomainName(kerberosConfig.getDomainName());
			objKerberosLogin.setKerberosKdc(kerberosConfig.getKerberosKdc());
			objKerberosLogin.setKerberosKeyTabContent(kerberosConfig.getKerberosKeyTabContent());
			objKerberosLogin.setKerberosLoginUser(dataSource.getHdfsHiveDetails().getKerberosLoginUser());
			objKerberosLogin.setKerberosPasswordServer(kerberosConfig.getKerberosPasswordServer());
			objKerberosLogin.setKerberosRealms(kerberosConfig.getKerberosRealms());
			objKerberosLogin.setKerbersoAdminServer(kerberosConfig.getKerberosAdminServer());

			JobSubmissionYarn objJobSubmissionYarn = new JobSubmissionYarn();
			objJobSubmissionYarn
					.setHadoopHostName("hdfs://" + dataSource.getCommonDetails().getServerName() + ":" + dataSource.getCommonDetails().getPortNumber());
			objJobSubmissionYarn.setHdpVersion(dataSource.getHdfsHiveDetails().getHdpVersion());
			objJobSubmissionYarn.setKerberosLoginUser(dataSource.getHdfsHiveDetails().getKerberosLoginUser());
			//objJobSubmissionYarn.setSparkYarnJar(dataSource.getSparkYarnJar());
			//objJobSubmissionYarn.setYarnRMAddress(dataSource.getYarnRMAddress());
			//objJobSubmissionYarn.setYarnRMPrincipal(dataSource.getYarnRMPrincipal());
			objJobSubmissionYarn.setKerberosRealm(kerberosConfig.getKerberosRealms());

			SparkYarnTestModel objSparkYarnTestModel = new SparkYarnTestModel();
			objSparkYarnTestModel.setObjJobSubmissionYarn(objJobSubmissionYarn);
			objSparkYarnTestModel.setObjKerberosLogin(objKerberosLogin);

			try {
				connectionStatus = sparkYarnSvc.getConnectionStatusWithKerberos(objSparkYarnTestModel);
			} catch (Exception e) {
				log.info("checkDataSourcesDetails, Exception occurred while checking connection : " + e.getMessage());
			}

			if (connectionStatus.equals("success") && (isCreate || isUpdate)) {

				if(Boolean.getBoolean(HelperTool.getEnv("is_spring_configserver_on", "false"))) {
					Map<String, String> encryptionMap = new HashMap<>();
					encryptionMap.put("KerberosLoginUser",
							ApplicationUtilities.getEncrypt(authorization, dataSource.getHdfsHiveDetails().getKerberosLoginUser()));
					encryptionMap.put("KerberosKeyTabContent",
							ApplicationUtilities.getEncrypt(authorization, kerberosConfig.getKerberosKeyTabContent()));
					encryptionMap.put("KerberosConfigFileContents",
							ApplicationUtilities.getEncrypt(authorization, kerberosConfig.getConfigFileContents()));
					log.info(
							"checkDataSourcesDetails(), Sending encrypted kerberos(Spark on Yarn) information to ConfigMgmt");
					enAssociation = ApplicationUtilities.doEncryptDecryptCommit(authorization, codeCloudAuthorization,
							dataSource.getDatasourceKey(), encryptionMap, mode);
					log.info(
							"checkDataSourcesDetails(), Successfully saved kerberos(Spark on Yarn) information in ConfigMgmt : "
									+ enAssociation);
					dataSource.getHdfsHiveDetails().setKerberosLoginUser(enAssociation);
				} else {
					Map<String, String> encryptionMap = new HashMap<>();
					encryptionMap.put("KerberosLoginUser", dataSource.getHdfsHiveDetails().getKerberosLoginUser());
					encryptionMap.put("KerberosKeyTabContent", kerberosConfig.getKerberosKeyTabContent());
					encryptionMap.put("KerberosConfigFileContents", kerberosConfig.getConfigFileContents());
					
					//enAssociation = Utilities.writeToCodeCloud(authorization, codeCloudAuthorization, dataSource.getDatasourceKey(), encryptionMap);
					enAssociation = ApplicationUtilities.writeToCodeCloudInMongo(user, dataSource.getDatasourceKey(), encryptionMap, mode);
					
					dataSource.getHdfsHiveDetails().setKerberosLoginUser(enAssociation);
				}
			}
		}
		// check null values in the parameters that are required for connecting
		// to hdp host
		else if (dataSource.getCategory().equals("hdfs")) {

			log.info("checkDataSourcesDetails, connection test for hdfs datasource will be initiated");
			if(mode.equalsIgnoreCase("validateConnection")) {
				InputStream in = hdpSvc.getSampleResults(user, authorization, null, datasourceKey, null);
				
				if (in != null && in.available() > 0) {
					return "success";
				} else {
					return "failed";
				}
			}

			String kerberosConfigFileName = dataSource.getHdfsHiveDetails().getKerberosConfigFileId();
			String kerberosKeyTabFileName = dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId();

			KerberosConfigInfo kerberosConfig = ApplicationUtilities.getKerberosConfigInfo(kerberosConfigFileName,
					kerberosKeyTabFileName);

			log.info("checkDataSourcesDetails, Received following Kerberos config info from ConfigFile : "
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

			try {
				connectionStatus = hdpSvc.getConnStatusWithKerberos(objKerberosLogin,
						"hdfs://" + dataSource.getCommonDetails().getServerName() + ":" + dataSource.getCommonDetails().getPortNumber(),
						dataSource.getHdfsHiveDetails().getHdfsFoldername(),dataSource.getReadWriteDescriptor());
				if (connectionStatus.equals("success")) {
					dataSource.setMetaData(hdpSvc.getMetadata(objKerberosLogin,
							"hdfs://" + dataSource.getCommonDetails().getServerName() + ":" + dataSource.getCommonDetails().getPortNumber(),
							dataSource.getHdfsHiveDetails().getHdfsFoldername()));
				}
			} catch (Exception e) {
				log.info("checkDataSourcesDetails, Exception occurred while checking connection : " + e.getMessage());
			}
			
			if (connectionStatus.equals("success") && (isCreate || (isUpdate && isKerberosInfoUpdated))) {
				if(Boolean.getBoolean(HelperTool.getEnv("is_spring_configserver_on", "false"))) {
					Map<String, String> encryptionMap = new HashMap<>();
					encryptionMap.put("KerberosLoginUser",
							ApplicationUtilities.getEncrypt(authorization, dataSource.getHdfsHiveDetails().getKerberosLoginUser()));
					encryptionMap.put("KerberosKeyTabContent",
							ApplicationUtilities.getEncrypt(authorization, kerberosConfig.getKerberosKeyTabContent()));
					encryptionMap.put("KerberosConfigFileContents",
							ApplicationUtilities.getEncrypt(authorization, kerberosConfig.getConfigFileContents()));
					log.info("checkDataSourcesDetails(), Sending encrypted kerberos(HDFS) information to ConfigMgmt");
					enAssociation = ApplicationUtilities.doEncryptDecryptCommit(authorization, codeCloudAuthorization,
							dataSource.getDatasourceKey(), encryptionMap, mode);
					log.info("checkDataSourcesDetails(), Successfully saved kerberos(HDFS) information in ConfigMgmt : "
							+ enAssociation);
					dataSource.getHdfsHiveDetails().setKerberosLoginUser(enAssociation);
				} else {
					Map<String, String> encryptionMap = new HashMap<>();
					encryptionMap.put("KerberosLoginUser", dataSource.getHdfsHiveDetails().getKerberosLoginUser());
					encryptionMap.put("KerberosKeyTabContent", kerberosConfig.getKerberosKeyTabContent());
					encryptionMap.put("KerberosConfigFileContents", kerberosConfig.getConfigFileContents());
					
					//enAssociation = Utilities.writeToCodeCloud(authorization, codeCloudAuthorization, dataSource.getDatasourceKey(), encryptionMap);
					enAssociation = ApplicationUtilities.writeToCodeCloudInMongo(user, dataSource.getDatasourceKey(), encryptionMap, mode);
					
					dataSource.getHdfsHiveDetails().setKerberosLoginUser(enAssociation);
				}
			}
		}
		// check null values in the parameters that are required for connecting
		// to File host
		else if (dataSource.getCategory().equals("file")) {
			log.info("checkDataSourcesDetails, connection test for File datasource will be initiated");
			FileConnectionModel objFileConnectionModel = new FileConnectionModel();
			objFileConnectionModel.setFileURL(dataSource.getFileDetails().getFileURL());
			objFileConnectionModel
					.setUsername(dataSource.getFileDetails().getFileServerUserName() != null ? dataSource.getFileDetails().getFileServerUserName() : "");
			objFileConnectionModel
					.setPassword(dataSource.getFileDetails().getFileServerUserPassword() != null ? dataSource.getFileDetails().getFileServerUserPassword() : "");

			try {
				connectionStatus = fileSvc.getConnectionStatus(objFileConnectionModel);
			} catch (Exception e) {
				log.info("checkDataSourcesDetails, Exception occurred while checking connection : " + e.getMessage());
			}

			if (connectionStatus.equals("success") && (isCreate || isUpdate)) {
				if(Boolean.getBoolean(HelperTool.getEnv("is_spring_configserver_on", "false"))) {
					dataSource.setMetaData(objFileConnectionModel.getMetaData());
					Map<String, String> encryptionMap = new HashMap<>();
					if ((dataSource.getFileDetails().getFileServerUserName() != null) && (dataSource.getFileDetails().getFileServerUserPassword() != null)) {
						encryptionMap.put("DbServerUsername",
								ApplicationUtilities.getEncrypt(authorization, dataSource.getFileDetails().getFileServerUserName()));
						encryptionMap.put("DbServerPassword",
								ApplicationUtilities.getEncrypt(authorization, dataSource.getFileDetails().getFileServerUserPassword()));
						log.info("checkDataSourcesDetails(), Sending encrypted FILE information to ConfigMgmt");
						enAssociation = ApplicationUtilities.doEncryptDecryptCommit(authorization, codeCloudAuthorization,
								dataSource.getDatasourceKey(), encryptionMap, mode);
						log.info("checkDataSourcesDetails(), Successfully saved FILE information in ConfigMgmt : "
								+ enAssociation);
						dataSource.getFileDetails().setFileServerUserName(enAssociation);
						dataSource.getFileDetails().setFileServerUserPassword(enAssociation);
					}
				} else {
					Map<String, String> encryptionMap = new HashMap<>();
					if ((dataSource.getFileDetails().getFileServerUserName() != null) && (dataSource.getFileDetails().getFileServerUserPassword() != null)) {
						encryptionMap.put("DbServerUsername", dataSource.getDbDetails().getDbServerUsername());
						encryptionMap.put("DbServerPassword", dataSource.getDbDetails().getDbServerPassword());
						
						//enAssociation = Utilities.writeToCodeCloud(authorization, codeCloudAuthorization, dataSource.getDatasourceKey(), encryptionMap);
						enAssociation = ApplicationUtilities.writeToCodeCloudInMongo(user, dataSource.getDatasourceKey(), encryptionMap, mode);
						
						dataSource.getFileDetails().setFileServerUserName(enAssociation);
						dataSource.getFileDetails().setFileServerUserPassword(enAssociation);
					}
				}
			}
		}
		// check null values in the parameters that are required for connecting
		// to mysql host
		else if (dataSource.getCategory().equals("jdbc")) {
			log.info("checkDataSourcesDetails, connection test for jdbc datasource will be initiated");
			JdbcConnectionModel objJdbcConnectionModel = new JdbcConnectionModel();
			objJdbcConnectionModel.setJdbcURL(dataSource.getDbDetails().getJdbcURL());
			objJdbcConnectionModel.setPassword(dataSource.getDbDetails().getDbServerPassword());
			objJdbcConnectionModel.setUsername(dataSource.getDbDetails().getDbServerUsername());

			try {
				connectionStatus = jdbcSvc.getConnectionStatus(objJdbcConnectionModel, dataSource.getDbDetails().getDbQuery(), dataSource.getReadWriteDescriptor());
			} catch (Exception e) {
				log.info("checkDataSourcesDetails, Exception occurred while checking connection : " + e.getMessage());
			}

			if (connectionStatus.equals("success") && (isCreate || isUpdate)) {
				if(Boolean.getBoolean(HelperTool.getEnv("is_spring_configserver_on", "false"))) {
					dataSource.setMetaData(objJdbcConnectionModel.getMetaData());
					Map<String, String> encryptionMap = new HashMap<>();
					encryptionMap.put("DbServerUsername",
							ApplicationUtilities.getEncrypt(authorization, dataSource.getDbDetails().getDbServerUsername()));
					encryptionMap.put("DbServerPassword",
							ApplicationUtilities.getEncrypt(authorization, dataSource.getDbDetails().getDbServerPassword()));
					log.info("checkDataSourcesDetails(), Sending encrypted JDBC information to ConfigMgmt");
					enAssociation = ApplicationUtilities.doEncryptDecryptCommit(authorization, codeCloudAuthorization,
							dataSource.getDatasourceKey(), encryptionMap, mode);
					dataSource.getDbDetails().setDbServerUsername(enAssociation);
					dataSource.getDbDetails().setDbServerPassword(enAssociation);
				} else {
					Map<String, String> encryptionMap = new HashMap<>();
					encryptionMap.put("DbServerUsername", dataSource.getDbDetails().getDbServerUsername());
					encryptionMap.put("DbServerPassword", dataSource.getDbDetails().getDbServerPassword());
					
					//enAssociation = Utilities.writeToCodeCloud(authorization, codeCloudAuthorization, dataSource.getDatasourceKey(), encryptionMap);
					enAssociation = ApplicationUtilities.writeToCodeCloudInMongo(user, dataSource.getDatasourceKey(), encryptionMap, mode);
					
					dataSource.getDbDetails().setDbServerUsername(enAssociation);
					dataSource.getDbDetails().setDbServerPassword(enAssociation);
					dataSource.setMetaData(objJdbcConnectionModel.getMetaData());
				}
			}
		} else if (dataSource.getCategory().equals("hive batch")) {

			KerberosConfigInfo kerberosConfig = null;

			log.info("checkDataSourcesDetails, connection test for hive datasource will be initiated");
			if(mode.equalsIgnoreCase("validateConnection")) {
				InputStream in = hiveBatchSvc.getSampleResults(user, authorization, null, datasourceKey);
				
				if (in != null && in.available() > 0) {
					return "success";
				} else {
					return "failed";
				}
			}

			String kerberosConfigFileName = dataSource.getHdfsHiveDetails().getKerberosConfigFileId();
			String kerberosKeyTabFileName = dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId();

			kerberosConfig = ApplicationUtilities.getKerberosConfigInfo(kerberosConfigFileName, kerberosKeyTabFileName);

			log.info("checkDataSourcesDetails, Received following Kerberos config info from ConfigFile : "
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

			try {
				connectionStatus = hiveBatchSvc.getConnectionStatusWithKerberos(objKerberosLogin,
						dataSource.getCommonDetails().getServerName(), String.valueOf(dataSource.getCommonDetails().getPortNumber()), dataSource.getHdfsHiveDetails().getQuery());
			} catch (Exception e) {
				log.info("checkDataSourcesDetails, Exception occurred while checking connection : " + e.getMessage());
			}
			if (connectionStatus.equals("success")) {
				dataSource.setMetaData(objKerberosLogin.getMetaData());
			}

			if (connectionStatus.equals("success") && (isCreate || (isUpdate && isKerberosInfoUpdated))) {
				if(Boolean.getBoolean(HelperTool.getEnv("is_spring_configserver_on", "false"))) {
					Map<String, String> encryptionMap = new HashMap<>();
					encryptionMap.put("KerberosLoginUser",
							ApplicationUtilities.getEncrypt(authorization, dataSource.getHdfsHiveDetails().getKerberosLoginUser()));
					encryptionMap.put("KerberosKeyTabContent",
							ApplicationUtilities.getEncrypt(authorization, kerberosConfig.getKerberosKeyTabContent()));
					encryptionMap.put("KerberosConfigFileContents",
							ApplicationUtilities.getEncrypt(authorization, kerberosConfig.getConfigFileContents()));
					log.info("checkDataSourcesDetails(), Sending encrypted kerberos(HIVE) information to ConfigMgmt");
					enAssociation = ApplicationUtilities.doEncryptDecryptCommit(authorization, codeCloudAuthorization,
							dataSource.getDatasourceKey(), encryptionMap, mode);
					log.info("checkDataSourcesDetails(), Successfully saved kereberos(HIVE) information in ConfigMgmt : "
							+ enAssociation);
					dataSource.getHdfsHiveDetails().setKerberosLoginUser(enAssociation);
				} else {
					Map<String, String> encryptionMap = new HashMap<>();
					encryptionMap.put("KerberosLoginUser", dataSource.getHdfsHiveDetails().getKerberosLoginUser());
					encryptionMap.put("KerberosKeyTabContent", kerberosConfig.getKerberosKeyTabContent());
					encryptionMap.put("KerberosConfigFileContents", kerberosConfig.getConfigFileContents());
					
					//enAssociation = Utilities.writeToCodeCloud(authorization, codeCloudAuthorization, dataSource.getDatasourceKey(), encryptionMap);
					enAssociation = ApplicationUtilities.writeToCodeCloudInMongo(user, dataSource.getDatasourceKey(), encryptionMap, mode);
					
					dataSource.getHdfsHiveDetails().setKerberosLoginUser(enAssociation);
				}
			}
		} else if (dataSource.getCategory().equals("hdfs batch")) {
			
			log.info("checkDataSourcesDetails, connection test for hdfs datasource will be initiated");
			if(mode.equalsIgnoreCase("validateConnection")) {
				InputStream in = hdpBatchSvc.getSampleResults(user, authorization, null, datasourceKey);
				
				if (in != null && in.available() > 0) {
					return "success";
				} else {
					return "failed";
				}
			}

			String kerberosConfigFileName = dataSource.getHdfsHiveDetails().getKerberosConfigFileId();
			String kerberosKeyTabFileName = dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId();

			KerberosConfigInfo kerberosConfig = ApplicationUtilities.getKerberosConfigInfo(kerberosConfigFileName,
					kerberosKeyTabFileName);

			log.info("checkDataSourcesDetails, Received following Kerberos config info from ConfigFile : "
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

			try {
				connectionStatus = hdpBatchSvc.getConnStatusWithKerberos(objKerberosLogin,
						"hdfs://" + dataSource.getCommonDetails().getServerName() + ":" + dataSource.getCommonDetails().getPortNumber(),
						dataSource.getHdfsHiveDetails().getHdfsFoldername());
				if (connectionStatus.equals("success")) {
					dataSource.setMetaData(hdpBatchSvc.getMetadata(objKerberosLogin,
							"hdfs://" + dataSource.getCommonDetails().getServerName() + ":" + dataSource.getCommonDetails().getPortNumber(),
							dataSource.getHdfsHiveDetails().getHdfsFoldername()));
				}
			} catch (Exception e) {
				log.info("checkDataSourcesDetails, Exception occurred while checking connection : " + e.getMessage());
			}

			if (connectionStatus.equals("success") && (isCreate || (isUpdate && isKerberosInfoUpdated))) {

				if(Boolean.getBoolean(HelperTool.getEnv("is_spring_configserver_on", "false"))) {
					Map<String, String> encryptionMap = new HashMap<>();
					encryptionMap.put("KerberosLoginUser",
							ApplicationUtilities.getEncrypt(authorization, dataSource.getHdfsHiveDetails().getKerberosLoginUser()));
					encryptionMap.put("KerberosKeyTabContent",
							ApplicationUtilities.getEncrypt(authorization, kerberosConfig.getKerberosKeyTabContent()));
					encryptionMap.put("KerberosConfigFileContents",
							ApplicationUtilities.getEncrypt(authorization, kerberosConfig.getConfigFileContents()));
					log.info("checkDataSourcesDetails(), Sending encrypted kerberos(HDFS) information to ConfigMgmt");
					enAssociation = ApplicationUtilities.doEncryptDecryptCommit(authorization, codeCloudAuthorization,
							dataSource.getDatasourceKey(), encryptionMap, mode);
					log.info("checkDataSourcesDetails(), Successfully saved kerberos(HDFS) information in ConfigMgmt : "
							+ enAssociation);
					dataSource.getHdfsHiveDetails().setKerberosLoginUser(enAssociation);
				} else {
					Map<String, String> encryptionMap = new HashMap<>();
					encryptionMap.put("KerberosLoginUser", dataSource.getHdfsHiveDetails().getKerberosLoginUser());
					encryptionMap.put("KerberosKeyTabContent", kerberosConfig.getKerberosKeyTabContent());
					encryptionMap.put("KerberosConfigFileContents", kerberosConfig.getConfigFileContents());
					
					//enAssociation = Utilities.writeToCodeCloud(authorization, codeCloudAuthorization, dataSource.getDatasourceKey(), encryptionMap);
					enAssociation = ApplicationUtilities.writeToCodeCloudInMongo(user, dataSource.getDatasourceKey(), encryptionMap, mode);
					
					dataSource.getHdfsHiveDetails().setKerberosLoginUser(enAssociation);
				}
			}
		} else {
			// Invalid datasource category. Please verify the input parameters.
			ApplicationUtilities.raiseConnectionFailedException(dataSource.getCategory());
		}

		return connectionStatus;
	}

	@Override
	public boolean validateDataSourceConnection(String user, String authorization, String codeCloudAuthorization,
			String dataSourceKey) throws DataSrcException, IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getMetadataContents(String user, String authorization, String dataSourceKey)
			throws DataSrcException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getDataSourceSamples(String user, String authorization, String datasourceKey,
			String hdfsFilename) throws DataSrcException, IOException, SQLException, ClassNotFoundException {
		
		log.info("ENTER:getDataSourcesSamples");
		log.info("getDataSourceSamples, user: " + user);
		log.info("getDataSourceSamples, datasourceKey: " + datasourceKey);
		
		//validate and throw Exceptions
		validateDataSource(user, datasourceKey);
		
		String namespace = null;

		ArrayList<String> dbDetails = dbUtilities.getDataSourceDetails(user, null, null, datasourceKey, null, true,
				false, authorization);
		
		JSONObject objJson = new JSONObject(dbDetails.get(0));
		log.info("DataSourceServiceImpl::getDataSourcesSamples:objJson:\n" + objJson);

		//EE-2904
		if(objJson.has("readWriteDescriptor")) {
			String readWriteFlag = objJson.getString("readWriteDescriptor");
			
			if("write".equals(readWriteFlag)) {
				//throw new CmlpDataSrcException("Sample Data cannot be fetched for 'write' type of ReadWrite flag.");
				String[] variables = {"readWriteDescriptor"};
				DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
				throw new DataSrcException(
						"Sample Data cannot be fetched for 'write' type of ReadWrite flag.", Status.BAD_REQUEST.getStatusCode(), err);

			}
		}

		// calling different service method based on category of the database
		if (objJson.getString("ownedBy").equals(user)) {

			if (objJson.getString("category").equals("mysql")) {

				log.info("RETURN:DataSourceServiceImpl::getDataSourcesSamples:mysql");
				return mySqlSvc.getSampleResults(user, authorization, namespace, datasourceKey);

			} else if (objJson.getString("category").equals("cassandra")) {

				log.info("RETURN:DataSourceServiceImpl::getDataSourcesSamples:cassandra");
				return cassandraSvc.getSampleResults(user, authorization, namespace, datasourceKey);

			} else if (objJson.getString("category").equals("file")) {

				log.info("RETURN:DataSourceServiceImpl::getDataSourcesSamples:File");
				return fileSvc.getSampleResults(user, authorization, namespace, datasourceKey);

			} else if (objJson.getString("category").equals("jdbc")) {

				log.info("RETURN:DataSourceServiceImpl::getDataSourcesSamples:jdbc");
				return jdbcSvc.getSampleResults(user, authorization, namespace, datasourceKey);
				
			} else if (objJson.getString("category").equals("mongo")) {

				log.info("RETURN:DataSourceServiceImpl::getDataSourcesSamples:mongo");
				return mongoSvc.getSampleResults(user, authorization, namespace, datasourceKey);

			} else if (objJson.getString("category").equals("hive")) { // return

				log.info("RETURN:DataSourceServiceImpl::getDataSourcesSamples:hive");
				return hiveSvc.getSampleResults(user, authorization, namespace, datasourceKey);

			} else if (objJson.getString("category").equals("hdfs")) { // return

				log.info("RETURN:DataSourceServiceImpl::hdfs");
				return hdpSvc.getSampleResults(user, authorization, namespace, datasourceKey,hdfsFilename);

			} else if (objJson.getString("category").equals("hdfs batch")) { // return

				log.info("RETURN:DataSourceServiceImpl::hdfs batch");
				return hdpBatchSvc.getSampleResults(user, authorization, namespace, datasourceKey);

			} else if (objJson.getString("category").equals("hive batch")) { // return

				log.info("RETURN:DataSourceServiceImpl::hive batch");
				return hiveBatchSvc.getSampleResults(user, authorization, namespace, datasourceKey);
			}
			
		} else {
			String[] variables = {"Authorization"};
			DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._1003, variables, null, CmlpApplicationEnum.DATASOURCE);
			throw new DataSrcException(
					"please check dataset key provided and user permission for this operation", Status.UNAUTHORIZED.getStatusCode(), err);

		}

		return null;
	}

	@Override
	public List<String> kerberosFileUpload(String user,
			MultipartFile[] bodyParts) throws DataSrcException, IOException {
		
		log.info("kerberosFileUpload, logged in user: " + user);
		List<String> uploadedFileNames = new ArrayList<String>();

		if (bodyParts != null) {
			// Make sure that there are two files to be uploaded
			if (bodyParts.length != 2) {
				String[] variables = {"uploadedFiles"};
				DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0005, variables, null, CmlpApplicationEnum.DATASOURCE);
				throw new DataSrcException(
						"Invalid Number of Attachments. Valid no. of attachemnst are - 2", Status.BAD_REQUEST.getStatusCode(), err);

			} else { // Make sure that one file must be .conf
				boolean isConfFile = false;
				String attachmentfilename = null;
				for (int i = 0; i < bodyParts.length; i++) {
					attachmentfilename = bodyParts[i].getOriginalFilename();
					if (attachmentfilename.endsWith(".conf")) {
						isConfFile = true;
						break;
					}
				}

				if (!isConfFile) {
					String[] variables = {".conf", ".keytab"};
					DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0006, variables, null, CmlpApplicationEnum.DATASOURCE);
					throw new DataSrcException(
							"Invalid format of Attachments. Valid formats are - .conf, .keytab", Status.BAD_REQUEST.getStatusCode(), err);

				}
			}

			// Make sure that file directory exists
			if (Files.notExists(Paths.get(HelperTool.getEnv("kerberos_user_config_dir",
					HelperTool.getComponentPropertyValue("kerberos_user_config_dir"))))) { // ./kerberos
				StringBuilder sb = new StringBuilder();
				sb.append(System.getProperty("user.dir")).append(System.getProperty("file.separator"))
						.append(HelperTool.getEnv("kerberos_user_config_dir",
								HelperTool.getComponentPropertyValue("kerberos_user_config_dir")));
				Files.createDirectories(Paths.get(sb.toString()));
			}

			MultipartFile attachment = null;
			String attachmentfilename = null;
			String fileNameKey = null;
			log.info("kerberosFileupload, Filename to be uploaded: " + fileNameKey);

			for (int i = 0; i < bodyParts.length; i++) {
					attachment = bodyParts[i];
					attachmentfilename = bodyParts[i].getOriginalFilename();
				fileNameKey = ApplicationUtilities.getKerberosFileName(user, attachmentfilename);
				if (attachmentfilename.endsWith(".conf")) { // Upload it as
					// m09286.krb5.conf
					StringBuilder sb = new StringBuilder();
					sb.append(System.getProperty("user.dir")).append(System.getProperty("file.separator"))
							.append(HelperTool.getEnv("kerberos_user_config_dir",
									HelperTool.getComponentPropertyValue("kerberos_user_config_dir")))
							.append(System.getProperty("file.separator")).append(fileNameKey).append(".krb5.conf");

						
						java.nio.file.Path path = FileSystems.getDefault().getPath(sb.toString());
						Files.copy(attachment.getInputStream(), path);
						

					uploadedFileNames.add(fileNameKey + ".krb5.conf");

				} else { // Upload the KeyTab with decoding
					if (attachmentfilename.lastIndexOf(".") > 0)
						attachmentfilename = attachmentfilename.substring(0, attachmentfilename.lastIndexOf("."));

					StringBuilder sb = new StringBuilder();
					sb.append(System.getProperty("user.dir")).append(System.getProperty("file.separator"))
							.append(HelperTool.getEnv("kerberos_user_config_dir",
									HelperTool.getComponentPropertyValue("kerberos_user_config_dir")))
							.append(System.getProperty("file.separator")).append(fileNameKey).append(".keytab");

						java.nio.file.Path path = FileSystems.getDefault().getPath(sb.toString());
						Files.copy(attachment.getInputStream(), path);
						//attachment.transferTo(new File(sb.toString()));
					uploadedFileNames.add(fileNameKey + ".keytab");
				}
			}

			log.info("kerberosFileupload, Files have been successfully uploaded.");
		} else {
			log.info("kerberosFileupload, No Files found to upload.");
		}

		return uploadedFileNames;
	}

	@Override
	public boolean writebackPrediction(String user, String authorization, String datasourceKey, String hdfsFilename,
			String data, String contentType, String includesHeader) throws DataSrcException, IOException {
		// TODO Auto-generated method stub
		return false;
	}

	private void validateDataSource(String user, String datasourceKey) throws DataSrcException, IOException {
		boolean isValid = false;
		
		isValid = dbUtilities.isValidDatasource(user, datasourceKey);
		
		if(!isValid) {
			if(dbUtilities.isDatasourceExists(datasourceKey)) {
				//unauthorized access
				String[] variables = {"Authorization"};
				DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._1003, variables, null, CmlpApplicationEnum.DATASOURCE);
				throw new DataSrcException(
						"please check dataset key provided and user permission for this operation", Status.UNAUTHORIZED.getStatusCode(), err);
				
			} else {
				//No Datasource in DB
				String[] variables = { "datasourceKey"};
				DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
				
				throw new DataSrcException("Invalid data. No Datasource info. Please send valid datasourceKey.",
						Status.NOT_FOUND.getStatusCode(), err);
			}
		}
	}

}
