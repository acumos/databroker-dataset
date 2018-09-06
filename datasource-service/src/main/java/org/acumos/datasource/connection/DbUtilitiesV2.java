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

package org.acumos.datasource.connection;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.core.Response.Status;

import org.acumos.config.WebConfig;
import org.acumos.datasource.common.CmlpApplicationEnum;
import org.acumos.datasource.common.CmlpErrorList;
import org.acumos.datasource.common.CmlpRestError;
import org.acumos.datasource.common.ErrorListEnum;
import org.acumos.datasource.common.Utilities;
import org.acumos.datasource.exception.CmlpDataSrcException;
import org.acumos.datasource.schema.ColumnMetadataInfo;
import org.acumos.datasource.schema.DataSourceMetadata;
//import org.acumos.service.service.JdbcDataSourceSvcImpl_v2;
import org.acumos.datasource.schema.DataSourceModelGet;
import org.acumos.datasource.schema.NameValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;

@Service
public class DbUtilitiesV2 {
	
	private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	
	private DBCollection datasrcCollection = null;
	
	@Autowired
	private Environment env;

	public DbUtilitiesV2() {
		super();
	}
	
	private DBCollection getMongoCollection() throws IOException {
		
		if(datasrcCollection == null) {
			String userName = env.getProperty("datasrc_mongo_username");
			String password = env.getProperty("datasrc_mongo_password");
			String dbName = env.getProperty("datasrc_mongo_dbname");
			String hostName = env.getProperty("datasrc_mongo_hostname");
			String portNumber = env.getProperty("datasrc_mongo_port");
			String collectionName = env.getProperty("datasrc_mongo_dbname");
			
			WebConfig config = new WebConfig();
			
			datasrcCollection = config.getDBCollection(userName, password, dbName, hostName, portNumber, collectionName);
		}
		
		return datasrcCollection;
	}
	
	
	private DBObject createDBObject(DataSourceModelGet dataSource, String mode) {
		log.info("createDBObject(),  initializing db object builder.");
		BasicDBObjectBuilder dataSrcBuilder = BasicDBObjectBuilder.start();

		log.info("createDBObject(),  initializing _id value.");
		dataSrcBuilder.append("_id", dataSource.getDatasourceKey());
		
		log.info("createDBObject(),  initializing datasource collection value.");
		if (dataSource.getDatasourceKey() != null) {
			dataSrcBuilder.append("datasourceKey", dataSource.getDatasourceKey());
		}
		
		if (dataSource.getCategory() != null) {
			dataSrcBuilder.append("category", dataSource.getCategory());
		}
		
		if (dataSource.getNamespace() != null) {
			dataSrcBuilder.append("namespace", dataSource.getNamespace());
		}
		
		if (dataSource.getDatasourceName() != null) {
			dataSrcBuilder.append("datasourceName", dataSource.getDatasourceName());
		}
		
		if (dataSource.getDatasourceDescription() != null) {
			dataSrcBuilder.append("datasourceDescription", dataSource.getDatasourceDescription());
		}
		
		if (dataSource.getReadWriteDescriptor() != null) {
			dataSrcBuilder.append("readWriteDescriptor", dataSource.getReadWriteDescriptor());
		}
		
		if (dataSource.getPredictorKey() != null) {
			dataSrcBuilder.append("predictorKey", dataSource.getPredictorKey());
		}
		
		if ("create".equals(mode)) {
			dataSrcBuilder.append("version", "v2");
		} else if (dataSource.getVersion() != null) {
			dataSrcBuilder.append("version", dataSource.getVersion());
		} 
		
		if ("create".equals(mode)) {
			dataSrcBuilder.append("isActive", true);
		}
		
		if (dataSource.getOwnedBy() != null) {
			dataSrcBuilder.append("ownedBy", dataSource.getOwnedBy());
		}
		
		if("create".equals(mode)) {
			dataSrcBuilder.append("createdTimestamp", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss z").format(Calendar.getInstance().getTime()));
		}
		
		if(!("create".equals(mode))) {
			dataSrcBuilder.append("updatedTimestamp", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss z").format(Calendar.getInstance().getTime()));
		}
		
		dataSrcBuilder.append("isDataReference", dataSource.isDataReference());
		
		if(dataSource.getCommonDetails() != null) {
			BasicDBObject commonDetails = new BasicDBObject();
			if (dataSource.getCommonDetails().getServerName() != null) {
				commonDetails.append("serverName", dataSource.getCommonDetails().getServerName());
			}
			
			commonDetails.append("portNumber", dataSource.getCommonDetails().getPortNumber());
			
			if (dataSource.getCommonDetails().getEnvironment() != null) {
				commonDetails.append("environment", dataSource.getCommonDetails().getEnvironment());
			}
			
			dataSrcBuilder.append("commonDetails", commonDetails);
			
		}
		
		if(dataSource.getFileDetails() != null) {
			
			BasicDBObject fileDetails = new BasicDBObject();
			
			if (dataSource.getFileDetails().getFileURL() != null) {
				fileDetails.append("fileURL", dataSource.getFileDetails().getFileURL());
			}
			
			if (dataSource.getFileDetails().getFileFormat() != null) {
				fileDetails.append("fileFormat", dataSource.getFileDetails().getFileFormat());
			}
			
			if (dataSource.getFileDetails().getFileDelimitor() != null) {
				fileDetails.append("fileDelimitor", dataSource.getFileDetails().getFileDelimitor());
			}
			
			if (dataSource.getFileDetails().getFileServerUserName() != null) {
				fileDetails.append("fileServerUserName", dataSource.getFileDetails().getFileServerUserName());
			}
			
			if (dataSource.getFileDetails().getFileServerUserPassword() != null) {
				fileDetails.append("fileServerUserPassword", dataSource.getFileDetails().getFileServerUserPassword());
			}
			
			if(!fileDetails.isEmpty()) {
				dataSrcBuilder.append("fileDetails", fileDetails);
			}
		}
		
		
		if(dataSource.getHdfsHiveDetails() != null) {
			
			BasicDBObject hdfsHiveDetails = new BasicDBObject();
			
			if (dataSource.getHdfsHiveDetails().getKerberosLoginUser() != null) {
				hdfsHiveDetails.append("kerberosLoginUser", dataSource.getHdfsHiveDetails().getKerberosLoginUser());
			}
			
			if (dataSource.getHdfsHiveDetails().getKerberosConfigFileId() != null) {
				hdfsHiveDetails.append("kerberosConfigFileId", 
						Utilities.getOriginalFileName(dataSource.getHdfsHiveDetails().getKerberosConfigFileId()));
			}
			
			if (dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId() != null) {
				hdfsHiveDetails.append("kerberosKeyTabFileId", 
						Utilities.getOriginalFileName(dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId()));
			}
			
			if (dataSource.getHdfsHiveDetails().getHdfsFoldername() != null) {
				hdfsHiveDetails.append("hdfsFoldername", dataSource.getHdfsHiveDetails().getHdfsFoldername());
			}
			
			if (dataSource.getHdfsHiveDetails().getHdpVersion() != null) {
				hdfsHiveDetails.append("hdpVersion", dataSource.getHdfsHiveDetails().getHdpVersion());
			}
			
			if (dataSource.getHdfsHiveDetails().getBatchSize() != null) {
				hdfsHiveDetails.append("batchSize", dataSource.getHdfsHiveDetails().getBatchSize());
			}
			
			if (dataSource.getHdfsHiveDetails().getQuery() != null) {
				hdfsHiveDetails.append("query", dataSource.getHdfsHiveDetails().getQuery());
			}
			
			if(!hdfsHiveDetails.isEmpty()) {
				dataSrcBuilder.append("hdfsHiveDetails", hdfsHiveDetails);
			}
		}
		
		if(dataSource.getDbDetails() != null) {
		
			BasicDBObject dbDetails = new BasicDBObject();
			
			if (dataSource.getDbDetails().getDatabaseName() != null) {
				dbDetails.append("databaseName", dataSource.getDbDetails().getDatabaseName());
			}
			
			if (dataSource.getDbDetails().getDbServerUsername() != null) {
				dbDetails.append("dbServerUsername", dataSource.getDbDetails().getDbServerUsername());
			}
			
			if (dataSource.getDbDetails().getDbServerPassword() != null) {
				dbDetails.append("dbServerPassword", dataSource.getDbDetails().getDbServerPassword());
			}
			
			if (dataSource.getDbDetails().getJdbcURL() != null) {
				dbDetails.append("jdbcURL", dataSource.getDbDetails().getJdbcURL());
			}
			
			if (dataSource.getDbDetails().getDbQuery() != null) {
				dbDetails.append("dbQuery", dataSource.getDbDetails().getDbQuery());
			}
			
			if (dataSource.getDbDetails().getDbCollectionName() != null) {
				dbDetails.append("dbCollectionName", dataSource.getDbDetails().getDbCollectionName());
			}
			
			if(!dbDetails.isEmpty()) {
				dataSrcBuilder.append("dbDetails", dbDetails);
			}
		}
		
		
		if(dataSource.getCustomMetaData() != null) {
			List<BasicDBObject> customMetaData = new ArrayList<>();
			
			for (NameValue detail : dataSource.getCustomMetaData()) {
				customMetaData.add((new BasicDBObject("name", detail.getName())).append("value", detail.getValue()));
			}
			
			dataSrcBuilder.append("customMetaData", customMetaData);
		}
		
		if(dataSource.getServiceMetaData() != null) {
			List<BasicDBObject> serviceMetaData = new ArrayList<>();
			
			for (NameValue detail : dataSource.getServiceMetaData()) {
				serviceMetaData.add((new BasicDBObject("name", detail.getName())).append("value", detail.getValue()));
			}
			
			dataSrcBuilder.append("serviceMetaData", serviceMetaData);
		}
		
		
		if (dataSource.getMetaData() != null) {

			BasicDBObjectBuilder metaBuilder = BasicDBObjectBuilder.start();
			if (dataSource.getMetaData().getMetaDataInfo() != null) {
				BasicDBObject metaDataInfo = new BasicDBObject();
				for (NameValue detail : dataSource.getMetaData().getMetaDataInfo()) {
					metaDataInfo.append(detail.getName(), detail.getValue());
				}
				
				metaBuilder.append("metaDataInfo", metaDataInfo);
			}
			
			//if(dataSource.getMetaData().getListIndex() != null) {
			//	metaBuilder.append("IndexMetadata", dataSource.getMetaData().getListIndex());
			//}
			
			if(dataSource.getMetaData() != null) {
					metaBuilder.append("IndexMetadata", dataSource.getMetaData().toString());
			}
			
			if (dataSource.getMetaData().getColumnMetaDataInfo() != null) {
				List<BasicDBObject> columnMetadataInfo = new ArrayList<>();
				for (ColumnMetadataInfo detail : dataSource.getMetaData().getColumnMetaDataInfo()) {
					columnMetadataInfo.add((new BasicDBObject("name", detail.getName()))
							.append("type", detail.getType()).append("size", detail.getSize()));
				}
				metaBuilder.append("columnMetadataInfo", columnMetadataInfo);
			}

			dataSrcBuilder.append("metaData", metaBuilder.get());
		}
		
		
		return dataSrcBuilder.get();
		
	}


	/**
	 * inserts a document into mongo
	 * 
	 * @param dataSource
	 * @throws Exception
	 */
	public void insertDataSourceDetails(DataSourceModelGet dataSource) throws Exception {
		WriteResult result = getMongoCollection().insert(createDBObject(dataSource, "create"));
		log.info("id of the inserted mongo object: " + result.getUpsertedId());
	}

	/**
	 * fetches a list of documents from mongo based on condition
	 * 
	 * @param user
	 * @param namespace
	 * @param category
	 * @param datasourceKey
	 * @param textSearch
	 * @param getEndpoint----when the request comes in for get REST endpoint. This criteria controls the json and removes username and password fields
	 * @return
	 * @throws Exception
	 */
	public ArrayList<String> getDataSourceDetails(String user, String namespace, String category, String datasourceKey,
			String textSearch, boolean getContentSearch, boolean getEndpoint, String authorization) throws Exception {
		ArrayList<String> retrieved = new ArrayList<String>();
		DBCursor cursor = null;
		BasicDBObjectBuilder query = BasicDBObjectBuilder.start();
		log.info("getDataSourceDetails(), checking inputs");

		if (textSearch != null) {
			return getDataSourceDetailsByTextSearch(user, textSearch);
		} else {
			if (user != null && !getContentSearch) {
				log.info("getDataSourceDetails(), checking user, get operation is being performed by " + user);
				query.add("ownedBy", user);
			}

			if (namespace != null) {
				log.info("getDataSourceDetails(), checking namespace, get operation is being performed for " + namespace
						+ " namespace.");
				query.add("namespace", namespace.toLowerCase());
			}

			if (category != null) {
				log.info("getDataSourceDetails(), checking category, get operation is being performed for " + category
						+ " category.");
				query.add("category", category.toLowerCase());
			}

			if (datasourceKey != null) {
				log.info("getDataSourceDetails(), checking category, get operation is being performed for id: "
						+ datasourceKey);
				query.add("_id", datasourceKey);
			}
		}
		query.add("isActive", true);
		
		log.info("getDataSourceDetails(), running query");
		cursor = getMongoCollection().find(query.get());

		log.info("getDataSourceDetails(), processing resultset");

		DBObject tempStorage;
		
		while (cursor.hasNext()) {
			tempStorage = cursor.next();
			
			tempStorage.removeField("_id");
			
			if((cursor.count() == 1) && getEndpoint && datasourceKey != null){
				if( tempStorage.containsField("dbDetails") ) {
					BasicDBObject dbDetails = (BasicDBObject)tempStorage.get("dbDetails");
					
					Map<String, String> decrptionMap = new HashMap<>();
					//decrptionMap = Utilities.getDecrypt(authorization, datasourceKey);
					decrptionMap = Utilities.readFromCodeCloud(authorization, datasourceKey);
					if (dbDetails.containsField("dbServerUsername") && decrptionMap.get("dbServerUsername".toLowerCase()) != null){
						dbDetails.put("dbServerUsername", decrptionMap.get("dbServerUsername".toLowerCase()));
					}
					if (dbDetails.containsField("dbServerPassword") && decrptionMap.get("dbServerPassword".toLowerCase()) != null){
						dbDetails.put("dbServerPassword", decrptionMap.get("dbServerPassword".toLowerCase()));
					}
					
					tempStorage.put("dbDetails", dbDetails);			
				}
				
				if( tempStorage.containsField("hdfsHiveDetails") ) {
					BasicDBObject hdfsDetails = (BasicDBObject)tempStorage.get("hdfsHiveDetails");
				
					Map<String, String> decrptionMap = new HashMap<>();
					//decrptionMap = Utilities.getDecrypt(authorization, datasourceKey);
					decrptionMap = Utilities.readFromCodeCloud(authorization, datasourceKey);
					if (hdfsDetails.containsField("kerberosLoginUser") && decrptionMap.get("KerberosLoginUser".toLowerCase()) != null){
						hdfsDetails.put("kerberosLoginUser", decrptionMap.get("KerberosLoginUser".toLowerCase()));
					}
					
					tempStorage.put("hdfsHiveDetails", hdfsDetails);			
				}
				
				if( tempStorage.containsField("fileDetails") ) {
					BasicDBObject fileDetails = (BasicDBObject)tempStorage.get("fileDetails");

					Map<String, String> decrptionMap = new HashMap<>();
					//decrptionMap = Utilities.getDecrypt(authorization, datasourceKey);
					decrptionMap = Utilities.readFromCodeCloud(authorization, datasourceKey);
					if (fileDetails.containsField("fileServerUserName") && decrptionMap.get("dbServerUsername".toLowerCase()) != null){
						fileDetails.put("fileServerUserName", decrptionMap.get("dbServerUsername".toLowerCase())); //encrypted as dbServerUsername
					}
					if (fileDetails.containsField("fileServerUserPassword") && decrptionMap.get("dbServerPassword".toLowerCase()) != null){
						fileDetails.put("fileServerUserPassword", decrptionMap.get("dbServerPassword".toLowerCase())); ////encrypted as dbServerPassword
					}
					
					tempStorage.put("fileDetails", fileDetails);			
				}
				
				retrieved.add(tempStorage.toString());
				break;
			} else if(tempStorage.containsField("kerberosDomainName")) {
				//ignore R1.65 datasources EE-2746
				continue;
			} else{
				//retrieved.add(cursor.next().toString());
				retrieved.add(tempStorage.toString());
			}
		}
		return retrieved;
	}

	/**
	 * fetches a list of documents from mongo based on condition
	 * 
	 * @param user
	 * @param textSearch
	 * @return
	 * @throws Exception
	 */

	private ArrayList<String> getDataSourceDetailsByTextSearch(String user, String textSearch) throws Exception {

		ArrayList<String> results = new ArrayList<String>();
		DBCursor cursor = null;

		BasicDBObjectBuilder query = BasicDBObjectBuilder.start();

		log.info("getDataSourceDetailsByTextSearch(), checking user, get operation is being performed by " + user);
		query.add("ownedBy", user);
		query.add("isActive", true);

		if (!textSearch.isEmpty()) {
			log.info("getDataSourceDetailsByTextSearch(), running mongodb query");

			cursor = getMongoCollection().find(query.get());

			log.info("Creating Regular Expression Pattern of the textSearch input" + textSearch);
			Pattern pattern = Pattern.compile(textSearch, Pattern.CASE_INSENSITIVE);
			Matcher m;
			String str_doc;

			log.info("getDataSourceDetailsByTextSearch(), processing resultset");
			while (cursor.hasNext()) {
				str_doc = cursor.next().toString();
				
				if(str_doc.indexOf("kerberosDomainName") > 0) //avoid r1.65 datasources EE-2746
					continue;
				
				m = pattern.matcher(str_doc);
				log.info("getDataSourceDetailsByTextSearch(), matching regex textSearch pattern");
				if (m.find()) {
					log.info(
							"getDataSourceDetailsByTextSearch(), document matching regex textSearch pattern added to results List");
					results.add(str_doc);
				}
			}

		}

		return results;
	}

	/**
	 * deletes a document from Mongo
	 * 
	 * @param user
	 * @param datasourceKey
	 * @return
	 * @throws Exception
	 */
	public boolean deleteDataSource(String user, String datasourceKey) throws Exception {
		boolean delete = false;
		DBCursor cursor = null;
		WriteResult result = null;

		log.info("deleteDataSource(), initializing query object");
		BasicDBObjectBuilder query = BasicDBObjectBuilder.start();

		query.add("ownedBy", user);
		query.add("_id", datasourceKey);

		log.info("deleteDataSource(), populating cursor with collection that is to be deleted");
		cursor = getMongoCollection().find(query.get());

		log.info("deleteDataSource(), checking cursor for value");
		if (cursor.hasNext()) {
			log.info("deleteDataSource(), issuing command to delete object with id: " + datasourceKey);
			result = getMongoCollection().remove(query.get());
			log.info("result for deletion: " + result.toString());
			delete = !result.isUpdateOfExisting();
		} else {
			log.info("deleteDataSource(), deletion failed for object with id: " + datasourceKey
					+ " .Please check datasource key provided and user persmission for this operation");
			String[] variables = {"datasourceKey"};
			CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
			
			throw new CmlpDataSrcException(
					"please check datasource key provided and user permission for this operation", Status.NOT_FOUND.getStatusCode(), err);
			
		}
		return delete;
	}

	/**
	 * soft deletes a document from Mongo sets the status parameter of the
	 * datasource to false
	 * 
	 * @param user
	 * @param datasourceKey
	 * @return
	 * @throws Exception
	 */
	public boolean softDeleteDataSource(String user, String datasourceKey) throws Exception {
		boolean delete = false;
		DBCursor cursor = null;
		WriteResult result = null;
		DBObject DBObj = null;

		log.info("softDeleteDataSource(), initializing query object");
		BasicDBObjectBuilder query = BasicDBObjectBuilder.start();

		query.add("ownedBy", user);
		query.add("_id", datasourceKey);

		log.info("softDeleteDataSource(), populating cursor with collection that is to be deleted");
		cursor = getMongoCollection().find(query.get());

		log.info("SoftDeleteDataSource(), checking cursor for value");
		if (cursor.hasNext()) {
			log.info("softDeleteDataSource(), issuing command to delete object with id: " + datasourceKey);
			DBObj = cursor.next();
			DBObj.put("isActive", new Boolean(false));
			result = getMongoCollection().update(query.get(), new BasicDBObject().append("$set", DBObj));
			log.info("result for deletion: " + result.toString());
			delete = result.isUpdateOfExisting();
		} else {
			log.info("softDeleteDataSource(), deletion failed for object with id: " + datasourceKey
					+ " .Please check datasource key provided and user persmission for this operation");
			String[] variables = {"datasourceKey"};
			CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
			
			throw new CmlpDataSrcException(
					"please check datasource key provided and user permission for this operation", Status.NOT_FOUND.getStatusCode(), err);
		}
		return delete;
	}

	/**
	 * Updates a documnet in mongo
	 * 
	 * @param user
	 * @param datasourceKey
	 * @param dataSource
	 * @return
	 * @throws Exception
	 */
	public boolean updateDataSource(String user, String datasourceKey, DataSourceModelGet dataSource) throws Exception {
		boolean update = false;
		WriteResult result = null;
		BasicDBObjectBuilder query = BasicDBObjectBuilder.start();

		log.info("updateDataSource(), checking user");
		if (user != null) {
			query.add("ownedBy", user);
		}

		log.info("updateDataSource(), checking datasourceKey");
		if (datasourceKey != null) {
			query.add("_id", datasourceKey);
		}

		if (!query.isEmpty()) {
			log.info("updateDataSource(), issuing command to update object with id: " + datasourceKey);
			result = getMongoCollection().update(query.get(),
					new BasicDBObject().append("$set", createDBObject(dataSource, "update")));
			
			update = result.isUpdateOfExisting();
		}
		
		return update;
	}
	
/*	public int getJdbcDatasetSize(ResultSet results) throws SQLException, CmlpDataSrcException {
		final int DATASET_SAMPLE_SIZE = 50;
		int size = 0;
		
		results.last();
		int rowCount = results.getRow();
		results.beforeFirst();
		
		if (rowCount <= DATASET_SAMPLE_SIZE) { 
			size = calculateJdbcResultSizeByNotSampling(results);
		} else {
			size = calculateJdbcResultSizeBySampling(results);
		}
		
		return size;
	}

	private int calculateJdbcResultSizeBySampling(ResultSet results) throws SQLException, CmlpDataSrcException {
		int size = 0;
		ResultSetMetaData rsmd = results.getMetaData();
		int columnCount = rsmd.getColumnCount();
	
		results.last();
		int rowCount = results.getRow();
		results.beforeFirst();
		
		int sampleSize = (int) Math.ceil(rowCount/5.0) ;
		int batchSize =  (int) Math.ceil(sampleSize/3.0) ;
		int middle = (int) Math.ceil(rowCount/2.0) ;
		
		StringBuilder resultString = new StringBuilder();
		int rowNumber = 1;
		while (results.next()) {
			if ((rowNumber >= 1 && rowNumber < 1 + batchSize) || 
				(rowNumber >= middle && rowNumber < middle + batchSize) ||
				(rowNumber >= (rowCount - batchSize + 1) && rowNumber <= batchSize)) {
				for (int i = 1; i <= columnCount; i++) {
					if (i > 1) {
						resultString.append(",");
					}
					int type = rsmd.getColumnType(i);	
					if (type == Types.VARCHAR || type == Types.CHAR || type == Types.LONGNVARCHAR) {
						resultString.append(results.getString(i));
					} else if (type == Types.BIT) {
						resultString.append(String.valueOf(results.getBoolean(i)));
					} else if (type == Types.BIGINT) {
						resultString.append(String.valueOf(results.getLong(i)));
					} else if (type == Types.NUMERIC || type == Types.DECIMAL) {
						resultString.append(String.valueOf(results.getBigDecimal(i)));
					} else if (type == Types.TINYINT || type == Types.SMALLINT || type == Types.INTEGER) {
						resultString.append(String.valueOf(results.getInt(i)));
					} else if (type == Types.REAL) {
						resultString.append(String.valueOf(results.getFloat(i)));
					} else if (type == Types.FLOAT || type == Types.DOUBLE) {
						resultString.append(String.valueOf(results.getDouble(i)));
					} else if (type == Types.BINARY || type == Types.LONGVARBINARY || type == Types.VARBINARY) {
						resultString.append(String.valueOf(results.getByte(i)));
					} else if (type == Types.DATE || type == Types.TIME || type == Types.TIMESTAMP) {
						resultString.append(String.valueOf(results.getTime(i)));
					} else {
						throw new CmlpDataSrcException("OOPS. Dev missed mapping. Please raise a bug.");
					}
				}	// for loop ends
			} // if expression ends
			resultString.append("\r\n");
			rowNumber++;
		}
		
		size = resultString.toString().getBytes().length;
		
		return size * 5;
	}
	
	private int calculateJdbcResultSizeByNotSampling(ResultSet results) throws SQLException, CmlpDataSrcException {
		int size = 0;
		ResultSetMetaData rsmd = results.getMetaData();
		int columnCount = rsmd.getColumnCount();
	
		results.last();
		int rowCount = results.getRow();
		
		results.beforeFirst();
		
		StringBuilder resultString = new StringBuilder();
		
		while ( results.next() ) {
			for (int i = 1; i <= columnCount; i++) {
				if (i > 1) {
					resultString.append(",");
				}
				int type = rsmd.getColumnType(i);	
				if (type == Types.VARCHAR || type == Types.CHAR || type == Types.LONGNVARCHAR) {
					resultString.append(results.getString(i));
				} else if (type == Types.BIT) {
					resultString.append(String.valueOf(results.getBoolean(i)));
				} else if (type == Types.BIGINT) {
					resultString.append(String.valueOf(results.getLong(i)));
				} else if (type == Types.NUMERIC || type == Types.DECIMAL) {
					resultString.append(String.valueOf(results.getBigDecimal(i)));
				} else if (type == Types.TINYINT || type == Types.SMALLINT || type == Types.INTEGER) {
					resultString.append(String.valueOf(results.getInt(i)));
				} else if (type == Types.REAL) {
					resultString.append(String.valueOf(results.getFloat(i)));
				} else if (type == Types.FLOAT || type == Types.DOUBLE) {
					resultString.append(String.valueOf(results.getDouble(i)));
				} else if (type == Types.BINARY || type == Types.LONGVARBINARY || type == Types.VARBINARY) {
					resultString.append(String.valueOf(results.getByte(i)));
				} else if (type == Types.DATE || type == Types.TIME || type == Types.TIMESTAMP) {
					resultString.append(String.valueOf(results.getTime(i)));
				} else {
					throw new CmlpDataSrcException("OOPS. Dev missed mapping. Please raise a bug.");
				}
			}
			resultString.append("\r\n");
		}

		size = resultString.toString().getBytes().length;
		
		return size;
	}*/
	

/*	public DataSourceMetadata getJdbcMetadata(ResultSet results) throws SQLException, IOException, CmlpDataSrcException {
		log.info("getJdbcMetadata()");
		ResultSetMetaData rsmd = results.getMetaData();
		int columnCount = rsmd.getColumnCount();
		// getting metaData
		ArrayList<NameValue> lstSimpleMetadata = new ArrayList<NameValue>();
		lstSimpleMetadata.add(new NameValue("columnCount", String.valueOf(columnCount)));
		
		results.last();
		
		lstSimpleMetadata.add(new NameValue("rowCount", String.valueOf(results.getRow())));
		// now you can move the cursor back to the top
		results.beforeFirst();
		//lstSimpleMetadata.add(new NameValue("rowCount", String.valueOf(rowCount)));
		
		long datasetSize = (new JdbcDataSourceSvcImpl_v2()).getJdbcDatasetSize(results);
		String value = String.valueOf(datasetSize) + " bytes";
		if (datasetSize == -1L) {
			value = "Not Available Due to BLOB columntype";
		}
		lstSimpleMetadata.add(new NameValue("dataSize", value));
		ArrayList<ColumnMetadataInfo> lstcolumnMetadata = new ArrayList<ColumnMetadataInfo>();
		for (int i = 1; i <= columnCount; i++) {
			lstcolumnMetadata.add(new ColumnMetadataInfo(rsmd.getColumnName(i), rsmd.getColumnTypeName(i),
					String.valueOf(rsmd.getColumnDisplaySize(i))));
		}
		DataSourceMetadata metadata = new DataSourceMetadata();
		metadata.setLstNameValuePairs(lstSimpleMetadata);
		metadata.setLstColumnMetadata(lstcolumnMetadata);
		System.out.println("JDBC Metadata: \n" + metadata);
		return metadata;
	}*/
	
	
	public DataSourceMetadata getJdbcMetadata(ResultSet results) throws SQLException, IOException, CmlpDataSrcException, Exception {
		log.info("getJdbcMetadata()");
		ResultSetMetaData rsmd = results.getMetaData();
		
		DataSourceMetadata metadata = new DataSourceMetadata();
		
		int columnCount = rsmd.getColumnCount();
		
		// getting metaData
		ArrayList<NameValue> lstSimpleMetadata = new ArrayList<NameValue>();
		lstSimpleMetadata.add(new NameValue("columnCount", String.valueOf(columnCount)));
		
		results.last();
		
		lstSimpleMetadata.add(new NameValue("rowCount", String.valueOf(results.getRow())));
		
		// now you can move the cursor back to the top
		results.beforeFirst();
		
//		long datasetSize = (new JdbcDataSourceSvcImpl_v2()).getJdbcDatasetSize(results);
		
		long datasetSize = 0L;
		
		String value = String.valueOf(datasetSize) + " bytes";
		if (datasetSize == -1L) {
			value = "Not Available Due to BLOB columntype";
		}
		
		lstSimpleMetadata.add(new NameValue("dataSize", value));
		
		ArrayList<ColumnMetadataInfo> lstcolumnMetadata = new ArrayList<ColumnMetadataInfo>();
		for (int i = 1; i <= columnCount; i++) {
			lstcolumnMetadata.add(new ColumnMetadataInfo(rsmd.getColumnName(i), rsmd.getColumnTypeName(i),
					String.valueOf(rsmd.getColumnDisplaySize(i))));
		}
		
		//metadata.setLstNameValuePairs(lstSimpleMetadata);
		//metadata.setLstColumnMetadata(lstcolumnMetadata);
		metadata.setMetaDataInfo(lstSimpleMetadata);
		metadata.setColumnMetaDataInfo(lstcolumnMetadata);
		
		System.out.println("JDBC Metadata: \n" + metadata);
		return metadata;
	}
		
	public DBObject getDataSourceDetailsByKey(String datasourceKey) throws Exception {
		
		BasicDBObjectBuilder query = BasicDBObjectBuilder.start();
		log.info("getDataSourceDetailsByKey(), checking inputs");
		
		if (datasourceKey != null) {
			log.info("getDataSourceDetailsByKey(), checking category, get operation is being performed for id: "
					+ datasourceKey);
			query.add("_id", datasourceKey);
		}

		query.add("isActive", true);
		
		log.info("getDataSourceDetailsByKey(), running query");
		DBObject result = getMongoCollection().findOne(query.get());

		return result;
	}
	
	
	public boolean isDatasourceExists(String datasourceKey) throws Exception {
		boolean isExists = false;
		
		if(datasourceKey == null || datasourceKey.isEmpty())
			return isExists;
		
		BasicDBObjectBuilder query = BasicDBObjectBuilder.start();

		log.info("isDatasourceExists(), performing based on datasetKey");
		
		query.add("datasourceKey", datasourceKey);
		query.add("isActive", true);
		
		//find _id only
		BasicDBObject fields = new BasicDBObject();
		fields.put("_id", 1);
		
		log.info("isDatasourceExists(), running query");
		DBObject dbResult = getMongoCollection().findOne(query.get(), fields);
		
		log.info("isDatasourceExists(), processing resultset");
		if(dbResult != null && dbResult.containsField("_id"))
			isExists = true;
			
		return isExists;
	}
	
	
	public boolean isValidDatasource(String user, String datasourceKey) throws Exception {
		boolean isExists = false;
		
		if(datasourceKey == null || datasourceKey.isEmpty())
			return isExists;
		
		if(user == null || user.isEmpty())
			return isExists;
		
		BasicDBObjectBuilder query = BasicDBObjectBuilder.start();

		log.info("isDatasourceExists(), performing based on datasetKey");
		
		query.add("datasourceKey", datasourceKey);
		query.add("ownedBy", user);
		query.add("isActive", true);
		
		//find _id only
		BasicDBObject fields = new BasicDBObject();
		fields.put("_id", 1);
		
		log.info("isDatasourceExists(), running query");
		DBObject dbResult = getMongoCollection().findOne(query.get(), fields);
		
		log.info("isDatasourceExists(), processing resultset");
		if(dbResult != null && dbResult.containsField("_id"))
			isExists = true;	
		
		return isExists;
	}

}
