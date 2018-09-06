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
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.core.Response.Status;

import org.slf4j.LoggerFactory;
import org.acumos.datasource.common.CmlpApplicationEnum;
import org.acumos.datasource.common.DataSrcErrorList;
import org.acumos.datasource.common.DataSrcRestError;
import org.acumos.datasource.common.ErrorListEnum;
import org.acumos.datasource.common.HelperTool;
import org.acumos.datasource.common.KerberosConfigInfo;
import org.acumos.datasource.connection.DbUtilitiesV2;
import org.acumos.datasource.exception.DataSrcException;
import org.acumos.datasource.model.KerberosLogin;
import org.acumos.datasource.schema.ColumnMetadataInfo;
import org.acumos.datasource.schema.DataSourceModelGet;
import org.acumos.datasource.schema.DataSourceMetadata;
import org.acumos.datasource.schema.NameValue;
import org.acumos.datasource.utils.ApplicationUtilities;
import org.slf4j.Logger;


@Service
public class HiveDataSourceSvcImpl implements HiveDataSourceSvc {

	private static Logger log = LoggerFactory.getLogger(HiveDataSourceSvcImpl.class);

	private Connection con;

	@Autowired
	private DbUtilitiesV2 dbUtilities;

	public HiveDataSourceSvcImpl() {
	}

	@Override
	public String getConnectionStatusWithKerberos(String hostName, String kerberosLoginUser, String port,
			String kerberosRealm, String query) throws ClassNotFoundException, SQLException, IOException, DataSrcException {

		String connectionStatus = "failed";

		// getting hive connection
		con = getConnection(hostName, kerberosLoginUser, port, kerberosRealm);
		Statement statement = con.createStatement();
		ResultSet results = null;
		statement.setMaxRows(1); //only need one row
		if (query != null) {
			results = statement.executeQuery(query);
		} else {
			results = statement.executeQuery("SELECT CURRENT_DATE");
		}

		log.info("got connection");

		// checking resultset
		if (results.next()) {
			connectionStatus = "success";
		}

		con.close();
		return connectionStatus;
	}

	@Override
	public void createKerberosKeytab(KerberosLogin objKerberosLogin) throws IOException, DataSrcException {
		log.info("Creating kerberos keytab for principal " + objKerberosLogin.getKerberosLoginUser());

		ApplicationUtilities.createKerberosKeytab(objKerberosLogin);

		log.info("Created kerberos keytab for principal " + objKerberosLogin.getKerberosLoginUser());

	}

	@Override
	public String getConnectionStatusWithKerberos(KerberosLogin objKerberosLogin, String hostName, String port,
			String query) throws ClassNotFoundException, SQLException, IOException, DataSrcException {

		log.info("Creating kerberos keytab for hostname " + hostName + " using principal "
				+ objKerberosLogin.getKerberosLoginUser() + " for hive connectivity testing.");
		//query = "select * from hiveorctest9";/*Utilities.createKerberosKeytab(objKerberosLogin);*/

		
		
		StringBuilder sb1 = new StringBuilder();
		sb1.append(System.getProperty("user.dir")).append(System.getProperty("file.separator"))
		  .append(objKerberosLogin.getKerberosLoginUser().substring(0, objKerberosLogin.getKerberosLoginUser().indexOf("@")))
		  .append(".kerberos.keytab");
	
		if(!Files.exists(Paths.get(sb1.toString()))) {
			log.info("Creating kerberos keytab for hostname " + hostName + " using principal "
					+ objKerberosLogin.getKerberosLoginUser() + " for hdfs connectivity testing.");

			ApplicationUtilities.createKerberosKeytab(objKerberosLogin, objKerberosLogin.getKerberosKeyTabFileName(), objKerberosLogin.getKerberosConfigFileName());	
		} else {
			try {
				log.info("overwriting kerberos keytab for hostname " + hostName + " using principal "
						+ objKerberosLogin.getKerberosLoginUser() + " for hdfs connectivity testing.");
				ApplicationUtilities.createKerberosKeytab(objKerberosLogin, objKerberosLogin.getKerberosKeyTabFileName(), objKerberosLogin.getKerberosConfigFileName());
			} catch (Exception e) {
				//ignore
				e.printStackTrace();
			}}
			

		log.info("Testing hive connectivity for hostname " + hostName + " using principal "
				+ objKerberosLogin.getKerberosLoginUser() + " after creating kerberos keytab");

//		String result = getConnectionStatusWithKerberos(hostName, objKerberosLogin.getKerberosLoginUser(), port,
//				objKerberosLogin.getKerberosRealms(), query);
		String result = getConnectionStatusMetadataWithKerberos(objKerberosLogin, hostName, port, query);

		log.info("test connectivity to hive resulted in " + result);
		return result;
	}

	@Override
	public Connection getConnection(String hostName, String kerberosLoginUser, String port, String kerberosRealm)
			throws IOException, ClassNotFoundException, SQLException, DataSrcException {
		log.info("Creating kerberised hadoop configuration for hostname " + hostName + " using principal "
				+ kerberosLoginUser + " for hive connectivity testing.");

		Configuration config = HelperTool.getKerberisedConfiguration(hostName,
				kerberosLoginUser);

		log.info("loading JDBC driver class for hive connectivity");
		Class.forName("org.apache.hive.jdbc.HiveDriver");

		//EE3488
		String transportMode=HelperTool.getEnv("hive_jdbc_transportMode", HelperTool.getComponentPropertyValue("hive_jdbc_transportMode"));
		String httpPath=HelperTool.getEnv("hive_jdbc_httpPath", HelperTool.getComponentPropertyValue("hive_jdbc_httpPath"));
		String connectionUri =null;
		log.info("getting JDBC hive connection using "
				+ "- hostname passed from client as payload - " + hostName 
				+ "- port passed from client as payload - " + port
				+ "- principal passed from client as payload - " + kerberosLoginUser
				+ "- transportMode from ENV or config properties - " + transportMode
				+ "- httpPath from ENV or config properties - " + httpPath
				+ " for hive connectivity testing.");
		if(transportMode != null && transportMode.equalsIgnoreCase("http")){
			connectionUri = "jdbc:hive2://" + hostName + ":" + port + "/;transportMode="+transportMode+";httpPath="+httpPath+";principal=hive/" + hostName + "@" + kerberosRealm;
		} else {
			connectionUri = "jdbc:hive2://" + hostName + ":" + port + "/;principal=hive/" + hostName + "@" + kerberosRealm;
		}
		//EE3488
		
		log.info("connection uri for hive: " + connectionUri);

		Connection con = DriverManager.getConnection(connectionUri);
		log.info("got JDBC hive connection for hostname " + hostName + " using principal " + kerberosLoginUser
				+ " for hive connectivity testing using URI: " + connectionUri);
		return con;
	}

	@Override
	public InputStream getResults(String user, String authorization, String namespace, String datasourceKey)
			throws DataSrcException, IOException, SQLException, ClassNotFoundException {

		ArrayList<String> dbDatasourceDetails = dbUtilities.getDataSourceDetails(user, null, null, datasourceKey, null, true, false, authorization);
		//JSONObject objJson = new JSONObject(hiveDatasourceDetails.get(0));
		DataSourceModelGet dbDataSource = ApplicationUtilities.getDataSourceModel(dbDatasourceDetails.get(0));
		
		//if (objJson.getString("category").equals("hive") && objJson.getString("ownedBy").equals(user)) {
		if (dbDataSource.getCategory().equals("hive") && dbDataSource.getOwnedBy().equals(user)) {
			//Map<String, String> decryptionMap = Utilities.readFromCodeCloud(authorization, datasourceKey);
			Map<String, String> decryptionMap = ApplicationUtilities.readFromMongoCodeCloud(user, datasourceKey);
			
			StringBuilder resultString = null;
			
			//if dataReferenceBox is checked, return config file/keytab file contents
			/*String isDataReference = objJson.optString("isDataReference");
			if (isDataReference != "" && isDataReference.equals("true")) {
				return getDataReference(objJson, decryptionMap);
			}
			
			log.info("getResults(), restoring config files from codecloud...");
			String kerberosConfigFileName = (Utilities.getKerberosFileName(user, objJson.getString("kerberosConfigFileId")) + ".krb5.conf");
			String kerberosKeyTabFileName = (Utilities.getKerberosFileName(user, objJson.getString("kerberosKeyTabFileId")) + ".keytab");
			*/

			if (dbDataSource.isDataReference()) {
				return getDataReference(dbDatasourceDetails.get(0), decryptionMap);
			}
			
			log.info("getResults(), restoring config files from codecloud...");
			String kerberosConfigFileName = (ApplicationUtilities.getKerberosFileName(user, dbDataSource.getHdfsHiveDetails().getKerberosConfigFileId()) + ".krb5.conf");
			String kerberosKeyTabFileName = (ApplicationUtilities.getKerberosFileName(user,  dbDataSource.getHdfsHiveDetails().getKerberosKeyTabFileId()) + ".keytab");

			//restore user exported config and KeyTab files
			restoreKerberosUserConfigFiles(kerberosConfigFileName, kerberosKeyTabFileName, decryptionMap);
			
			//Restore Kerberos Cache config files from code cloud if they missed due to POD bounce
			restoreKerberosCacheConfigFiles(decryptionMap);
			
			//successfully Restored the files
			log.info("getResults(), Kerberos files were restored successfully...Proceed with check connection");
			
			KerberosConfigInfo kerberosConfig = ApplicationUtilities.getKerberosConfigInfo(kerberosConfigFileName,
					kerberosKeyTabFileName);

			/*con = getConnection(objJson.getString("serverName"),
					decryptionMap.get("kerberosLoginUser".toLowerCase()), objJson.getString("portNumber"),
					//decryptionMap.get("kerberosRealms".toLowerCase()));
					kerberosConfig.getKerberosRealms());*/
			
			con = getConnection(dbDataSource.getCommonDetails().getServerName(),
					decryptionMap.get("kerberosLoginUser".toLowerCase()), String.valueOf(dbDataSource.getCommonDetails().getPortNumber()),
					//decryptionMap.get("kerberosRealms".toLowerCase()));
					kerberosConfig.getKerberosRealms());
			
			//Remove semicolon
			dbDataSource.getHdfsHiveDetails().setQuery(ApplicationUtilities.trimSemicolonAtEnd(dbDataSource.getHdfsHiveDetails().getQuery()));
			
			//preparing statement and executing it
			Statement statement = con.createStatement();
			ResultSet results = statement.executeQuery(dbDataSource.getHdfsHiveDetails().getQuery());
			
			resultString = new StringBuilder();
			
			//populating metaddata
			ResultSetMetaData rsmd = results.getMetaData();
			int columnCount = rsmd.getColumnCount();
			
			//appending column names
			for (int i = 1; i <= columnCount; i++) {
					resultString.append(rsmd.getColumnName(i)); 
					if (i==columnCount) {
						resultString.append("");
					} else {
					resultString.append(",");
				}
			}
			resultString.append("\r\n");
			
			//browsing through resultset
			while (results.next()) {
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
						resultString.append(results.getTimestamp(i));
					} else {
						DataSrcRestError err = DataSrcErrorList.buildError(new Exception("OOPS. Dev missed mapping for DATA Type.. Please raise a bug."), null, CmlpApplicationEnum.DATASOURCE);
						throw new DataSrcException("OOPS. Dev missed mapping. Please raise a bug.",
								Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
					}
				}
				resultString.append("\r\n");
			}
			return new ByteArrayInputStream(resultString.toString().getBytes());
		} 
		
		//No information
		String[] variables = {"datasourceKey"};
		
		DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
		
		throw new DataSrcException("No Results found for the given DatasourceKey.",
				Status.NOT_FOUND.getStatusCode(), err);
	
	}
	private InputStream getDataReference(String jsonStr, Map<String, String> decryptionMap) throws DataSrcException {
			if (decryptionMap.size() == 0) {
				DataSrcRestError err = DataSrcErrorList.buildError(new Exception("Unable to retrieve decryption files"), null, CmlpApplicationEnum.DATASOURCE);
				
				throw new DataSrcException("Exception occurred during GET.",
						Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
			}
			
			JSONObject objJson = new JSONObject(jsonStr);
			
			//remove unwanted fields
			objJson.remove("kerberosConfigFileId");
			objJson.remove("kerberosKeyTabFileId");
			objJson.remove("kerberosLoginUser");
			objJson.remove("_id");
			
			//Add the fields from code cloud
			objJson.put("kerberosconfigfilecontents", ApplicationUtilities.htmlDecoding(decryptionMap.get("kerberosconfigfilecontents")));
					//StringEscapeUtils.unescapeHtml(decryptionMap.get("kerberosconfigfilecontents")).replaceAll("@@@",
						//	""));
			objJson.put("kerberoskeytabcontent", decryptionMap.get("kerberoskeytabcontent"));
			objJson.put("kerberosloginuser", decryptionMap.get("kerberosloginuser"));
			
			return new ByteArrayInputStream(objJson.toString().getBytes());
		}

	//calling this method will move the cursor to the last row in this Resultset
	private DataSourceMetadata getMetadata(ResultSet results, int rowCount) throws SQLException, IOException, DataSrcException {
		ResultSetMetaData rsmd = results.getMetaData();
		int columnCount = rsmd.getColumnCount();
		// getting metaData
		ArrayList<NameValue> lstSimpleMetadata = new ArrayList<NameValue>();
		lstSimpleMetadata.add(new NameValue("columnCount", String.valueOf(columnCount)));
			
		long dataSize = getHiveDatasetSize(results, rowCount);
		
		String value = String.valueOf(dataSize) + " bytes";
		if (dataSize == -1L) {
			value = "Not Available Due to BLOB columntype";
		}
		lstSimpleMetadata.add(new NameValue("dataSize", value));
		
		// countRow
//		int rowCount = 0;
//		rowCount = getHiveRowCount(results);

		lstSimpleMetadata.add(new NameValue("rowCount", String.valueOf(rowCount)));
		
		ArrayList<ColumnMetadataInfo> lstcolumnMetadata = new ArrayList<ColumnMetadataInfo>();
		for (int i = 1; i <= columnCount; i++) {
			lstcolumnMetadata.add(new ColumnMetadataInfo(rsmd.getColumnName(i), rsmd.getColumnTypeName(i),
					String.valueOf(rsmd.getColumnDisplaySize(i))));
		}
		DataSourceMetadata metadata = new DataSourceMetadata();
		metadata.setMetaDataInfo(lstSimpleMetadata);
		metadata.setColumnMetaDataInfo(lstcolumnMetadata);
		System.out.println("Hive Metadata: \n" + metadata);
		return metadata;
	}

//	@Override
	private String getConnectionStatusMetadataWithKerberos(KerberosLogin objKerberosLogin, String hostName, String port,
			 String query) throws ClassNotFoundException, SQLException, IOException, DataSrcException {
			
		String connectionStatus = "failed";
		//query = "select * from hiveorctest9";
		log.info("query=" + query);
		// getting hive connection
		con = getConnection(hostName, objKerberosLogin.getKerberosLoginUser(), port, objKerberosLogin.getKerberosRealms());
		Statement statement = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		ResultSet results = null;
		if (query != null) {
			try {
				results = statement.executeQuery(query);
			} catch (SQLException e) {
				String[] variables = {"query"};
				DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
				
				throw new DataSrcException(
						"Something wrong with query.", Status.BAD_REQUEST.getStatusCode(), err);
			}
			int rowCount = getHiveRowCount(results);
			
			//Execute the query second time to reset the current row in the beginning
			
			results = statement.executeQuery(query);
			objKerberosLogin.setMetaData(getMetadata(results, rowCount));
			connectionStatus = "success";
		} else {
			results = statement.executeQuery("SELECT CURRENT_DATE");
			if (results.next()) {
				connectionStatus = "success";
			}
		}
	
		log.info("got connection");
	
		con.close();
		return connectionStatus;
	}
	
	@Override
	public InputStream getSampleResults
			(String user, String authorization, String namespace, String datasourceKey)
			throws DataSrcException, IOException, SQLException, ClassNotFoundException {

		ArrayList<String> dbDatasourceDetails = dbUtilities.getDataSourceDetails(user, null, null, datasourceKey, null, true, false, authorization);
		DataSourceModelGet dbDataSource = ApplicationUtilities.getDataSourceModel(dbDatasourceDetails.get(0));
		
		if (dbDataSource.getCategory().equals("hive") && dbDataSource.getOwnedBy().equals(user)) {
			//Map<String, String> decryptionMap = Utilities.readFromCodeCloud(authorization, datasourceKey);
			Map<String, String> decryptionMap = ApplicationUtilities.readFromMongoCodeCloud(user, datasourceKey);
			
			StringBuilder resultString = null;
			
			log.info("getSampleResults(), restoring config files from codecloud...");
			String kerberosConfigFileName = (ApplicationUtilities.getKerberosFileName(user, dbDataSource.getHdfsHiveDetails().getKerberosConfigFileId()) + ".krb5.conf");
			String kerberosKeyTabFileName = (ApplicationUtilities.getKerberosFileName(user, dbDataSource.getHdfsHiveDetails().getKerberosKeyTabFileId()) + ".keytab");
			
			//restore user exported config and KeyTab files
			restoreKerberosUserConfigFiles(kerberosConfigFileName, kerberosKeyTabFileName, decryptionMap);
			
			//Restore Kerberos Cache config files if they missed due to POD bounce
			restoreKerberosCacheConfigFiles(decryptionMap);
			
			//successfully Restored the files
			log.info("getSampleResults(), Kerberos files were restored successfully...Proceed with check connection");
			
			KerberosConfigInfo kerberosConfig = ApplicationUtilities.getKerberosConfigInfo(kerberosConfigFileName,
					kerberosKeyTabFileName);

			con = getConnection(dbDataSource.getCommonDetails().getServerName(),
					decryptionMap.get("kerberosLoginUser".toLowerCase()), String.valueOf(dbDataSource.getCommonDetails().getPortNumber()),
					//decryptionMap.get("kerberosRealms".toLowerCase()));
					kerberosConfig.getKerberosRealms());
			
			//Remove semicolon
			dbDataSource.getHdfsHiveDetails().setQuery(ApplicationUtilities.trimSemicolonAtEnd(dbDataSource.getHdfsHiveDetails().getQuery()));
			
			//preparing statement and executing it
			Statement statement = con.createStatement();
			statement.setMaxRows(5);  //only 5 rows
			ResultSet results = statement.executeQuery(dbDataSource.getHdfsHiveDetails().getQuery());
			
			resultString = new StringBuilder();
			
			//populating metaddata
			ResultSetMetaData rsmd = results.getMetaData();
			int columnCount = rsmd.getColumnCount();
			
			//appending column names
			for (int i = 1; i <= columnCount; i++) {
					resultString.append(rsmd.getColumnName(i)); 
					if (i==columnCount) {
						resultString.append("");
					} else {
					resultString.append(",");
				}
			}

			resultString.append("\r\n");
			
			//browsing through resultset
			while (results.next()) {
				for (int i = 1; i <= columnCount; i++) {
					if (i > 1) {
						resultString.append(",");
					}
					int type = rsmd.getColumnType(i);
					
					switch(type) {
						case(Types.VARCHAR) :
						case(Types.CHAR) :
						case(Types.LONGNVARCHAR) :
							resultString.append(results.getString(i));
							break;
						case(Types.BIT) :
							resultString.append(String.valueOf(results.getBoolean(i)));
							break;
						case(Types.BIGINT) :
							resultString.append(String.valueOf(results.getLong(i)));
							break;
						case(Types.NUMERIC) :
						case(Types.DECIMAL) :
							resultString.append(String.valueOf(results.getBigDecimal(i)));
							break;
						case(Types.TINYINT) :
						case(Types.SMALLINT) :
						case(Types.INTEGER) :
							resultString.append(String.valueOf(results.getInt(i)));
							break;
						case(Types.REAL) :
							resultString.append(String.valueOf(results.getFloat(i)));
							break;
						case(Types.FLOAT) :
						case(Types.DOUBLE) :
							resultString.append(String.valueOf(results.getDouble(i)));
							break;
						case(Types.BINARY) :
						case(Types.LONGVARBINARY) :
						case(Types.VARBINARY) :
							resultString.append(String.valueOf(results.getByte(i)));
							break;
						case(Types.DATE) :
						case(Types.TIME) :
						case(Types.TIMESTAMP) :
							resultString.append(results.getTimestamp(i));
							break;
						default:
							DataSrcRestError err = DataSrcErrorList.buildError(new Exception("OOPS. Dev missed mapping for DATA Type.. Please raise a bug."), null, CmlpApplicationEnum.DATASOURCE);
							throw new DataSrcException("OOPS. Dev missed mapping. Please raise a bug.",
									Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
					}
				}
				resultString.append("\r\n");
			}
			
			//delete the restored files
			ApplicationUtilities.deleteUserKerberosConfigFiles(kerberosConfigFileName, kerberosKeyTabFileName);
			
			return new ByteArrayInputStream(resultString.toString().getBytes());
		}

		//No information
		String[] variables = {"datasourceKey"};
		
		DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
		
		throw new DataSrcException("No Results found for the given DatasourceKey.",
				Status.NOT_FOUND.getStatusCode(), err);
	}
	
	private int getHiveRowCount(ResultSet results) throws SQLException {
		// countRow
		int rowCount = 0;
		while (results.next()) {
			rowCount++;
		}
		return rowCount;
	}
	
	public long getHiveDatasetSize(ResultSet results, int rowCount) throws SQLException, IOException, DataSrcException {
		log.info("ENTER:getHiveDatasetSize()");

		//final int DATASET_SAMPLE_SIZE = 50;
		int MIN_SAMPLING_SIZE; //default to 50
		String strMIN_SAMPLING_SIZE = HelperTool.getEnv("minimum_sampling_size",
				HelperTool.getComponentPropertyValue("minimum_sampling_size"));
		MIN_SAMPLING_SIZE = strMIN_SAMPLING_SIZE != null ? Integer.parseInt(strMIN_SAMPLING_SIZE) : 50;
		
		/*
		 * NOT SUPPORTED HIVE JDBC
		 * 	results.last();
			int rowCount = results.getRow();
			results.beforeFirst();
		 */
		
		// countRow
//		int rowCount = 0;
//		rowCount = getHiveRowCount(results);
		long size = 0L;
		if (rowCount <= MIN_SAMPLING_SIZE) { 
			size = calculateHiveResultSizeByNotSampling(results, rowCount);
		} else {
			size = calculateHiveResultSizeBySampling(results, rowCount);
		}
		
		return size;
	}
	
	private long calculateHiveResultSizeByNotSampling(ResultSet results, int rowCount) throws SQLException, DataSrcException {
		log.info("calculateHiveResultSizeByNotSampling()");

		ResultSetMetaData rsmd = results.getMetaData();
		int columnCount = rsmd.getColumnCount();

		// countRow
//		int rowCount = 0;
//		rowCount = getHiveRowCount(results);
				
		log.info("Number of Records: " + rowCount);
		
		StringBuilder resultString = new StringBuilder();
		int rowNumber = 1;

		while (results.next()) {
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
				/* } else if (type == Types.BLOB) {
					//Hive doesnot support any BLOB, CLOB AND NCLOB data types YET
					return -1L; */
				} else {
					DataSrcRestError err = DataSrcErrorList.buildError(new Exception("OOPS. Dev missed mapping for DATA Type.. Please raise a bug."), null, CmlpApplicationEnum.DATASOURCE);
					throw new DataSrcException("OOPS. Dev missed mapping. Please raise a bug.",
							Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
				}
			}
			resultString.append("\r\n");
			rowNumber++;
		}

		log.info("Past Last Record: " + rowNumber);
		long size = 0L;
		size = resultString.toString().getBytes().length * 1L;
		log.info("Size of 100% dataset: " + size + " bytes");
		return size;
	}
	
	private long calculateHiveResultSizeBySampling(ResultSet results, int rowCount) throws SQLException, DataSrcException {
		log.info("calculateHiveResultSizeBySampling()");

		ResultSetMetaData rsmd = results.getMetaData();
		int columnCount = rsmd.getColumnCount();

		int sampleSize = (int) Math.ceil(rowCount / 5.0);
		int batchSize = (int) Math.ceil(sampleSize / 3.0);
		int middle = (int) Math.ceil(rowCount / 2.0);

		log.info("Number of Records: " + rowCount);
		log.info("SamplingSize: " + sampleSize);
		log.info("BatchSize: " + batchSize);
		log.info("Middle rowNumber: " + middle);
		
		StringBuilder resultString = new StringBuilder();
		int rowNumber = 1;
		while (results.next()) {
			if ((rowNumber >= 1 && rowNumber < 1 + batchSize) || (rowNumber >= middle && rowNumber < middle + batchSize)
					|| (rowNumber >= (rowCount - batchSize + 1) && rowNumber <= rowCount)) {
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
					} /*else if (type == Types.BLOB) {
						return -1L;
						Hive doesn't support BLOB, CLOB, NCLOB datatype YET
					} */else {
						DataSrcRestError err = DataSrcErrorList.buildError(new Exception("OOPS. Dev missed mapping for DATA Type.. Please raise a bug."), null, CmlpApplicationEnum.DATASOURCE);
						throw new DataSrcException("OOPS. Dev missed mapping. Please raise a bug.",
								Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
					}
				} // for loop ends
			} // if expression ends
			resultString.append("\r\n");
			rowNumber++;
		}
		log.info("Past Last Record: " + rowNumber);
		long size = 0;
		size = resultString.toString().getBytes().length * 1L;
		log.info("Size of 20% Dataset sampling : " + size + " bytes");
		return size * 5;
	}
	
	private void restoreKerberosCacheConfigFiles(Map<String, String> decryptionMap) throws DataSrcException {
		String krbLoginUser = decryptionMap.get("kerberosLoginUser".toLowerCase());
		
		StringBuilder sb1 = new StringBuilder();
		sb1.append(krbLoginUser.substring(0, krbLoginUser.indexOf("@"))).append(".krb5.conf");
		
		StringBuilder sb2 = new StringBuilder();
		sb2.append(krbLoginUser.substring(0, krbLoginUser.indexOf("@"))).append(".kerberos.keytab");
		
		if(!ApplicationUtilities.isUserKerberosCacheConfigFilesExists(sb1.toString(), sb2.toString())) {
			//Restore Kerberos Cache files if they are missed due to POD bounce
			log.info("getSampleResults(), restoring config files from codecloud...");
			
			boolean isWritten = ApplicationUtilities.writeToKerberosCacheFile(sb1.toString(), decryptionMap.get("KerberosConfigFileContents".toLowerCase()));
			
			if(isWritten) {
				isWritten = ApplicationUtilities.writeToKerberosCacheFile(sb2.toString(), decryptionMap.get("KerberosKeyTabContent".toLowerCase()));
			}
			
			if(!isWritten) {
				//1. delete config files, if any
				ApplicationUtilities.deleteUserKerberosCacheConfigFiles(sb1.toString(), sb2.toString());
				
				//2. throw Exception
				DataSrcRestError err = DataSrcErrorList.buildError(new Exception("Unable to retrieve connection parameters from the codecloud."), null, CmlpApplicationEnum.DATASOURCE);
				
				throw new DataSrcException("Exception occurred during GET.",
						Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
			}
		}
	}
	
	private void restoreKerberosUserConfigFiles(String krbConfigFileName, String krbKeyTabFileName, Map<String, String> decryptionMap) throws DataSrcException {
		log.info("getSampleResults(), restoring config files from codecloud...");
		boolean isWritten = ApplicationUtilities.writeToKerberosFile(krbConfigFileName, decryptionMap.get("KerberosConfigFileContents".toLowerCase()));
		
		if(isWritten) {
			isWritten = ApplicationUtilities.writeToKerberosFile(krbKeyTabFileName, decryptionMap.get("KerberosKeyTabContent".toLowerCase()));
		}
		
		if(!isWritten) {
			//1. delete config files, if any
			ApplicationUtilities.deleteUserKerberosConfigFiles(krbConfigFileName, krbKeyTabFileName);
			
			//2. throw Exception
			DataSrcRestError err = DataSrcErrorList.buildError(new Exception("Unable to retrieve connection parameters from the codecloud."), null, CmlpApplicationEnum.DATASOURCE);
			throw new DataSrcException("OOPS. Dev missed mapping. Please raise a bug.",
					Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
		}
	}

	@Override
	public InputStream getSampleResults(DataSourceModelGet dataSource) throws DataSrcException, IOException, SQLException, ClassNotFoundException {
		//check for connection parameters
		ApplicationUtilities.validateConnectionParameters(dataSource);
		
		String kerberosConfigFileName = dataSource.getHdfsHiveDetails().getKerberosConfigFileId();
		String kerberosKeyTabFileName = dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId();

		KerberosConfigInfo kerberosConfig = ApplicationUtilities.getKerberosConfigInfo(kerberosConfigFileName, kerberosKeyTabFileName);

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

		ApplicationUtilities.createKerberosKeytab(objKerberosLogin, objKerberosLogin.getKerberosKeyTabFileName(), objKerberosLogin.getKerberosConfigFileName());	
	} else {
		try {
			log.info("overwriting kerberos keytab for hostname " + dataSource.getCommonDetails().getServerName() + " using principal "
					+ objKerberosLogin.getKerberosLoginUser() + " for hdfs connectivity testing.");
			ApplicationUtilities.createKerberosKeytab(objKerberosLogin, objKerberosLogin.getKerberosKeyTabFileName(), objKerberosLogin.getKerberosConfigFileName());
		} catch (Exception e) {
			//ignore
			e.printStackTrace();
		}
	}

		con = getConnection(dataSource.getCommonDetails().getServerName(), dataSource.getHdfsHiveDetails().getKerberosLoginUser(), String.valueOf(dataSource.getCommonDetails().getPortNumber()),
				// decryptionMap.get("kerberosRealms".toLowerCase()));
				kerberosConfig.getKerberosRealms());

		//connectivity test passed
		if("write".equalsIgnoreCase(dataSource.getReadWriteDescriptor())) {
			//validate connection call as there is no getSamples() allowed on write flag
			return (new  ByteArrayInputStream("Success".getBytes()));
		}
		
		//Remove semicolon
		dataSource.getHdfsHiveDetails().setQuery(ApplicationUtilities.trimSemicolonAtEnd(dataSource.getHdfsHiveDetails().getQuery()));
				

		// preparing statement and executing it
		Statement statement = con.createStatement();
		statement.setMaxRows(5); // only 5 rows
		ResultSet results = statement.executeQuery(dataSource.getHdfsHiveDetails().getQuery());

		InputStream sample = populatingSample(results);

		// delete the restored files
		ApplicationUtilities.deleteUserKerberosConfigFiles(dataSource.getHdfsHiveDetails().getKerberosConfigFileId(),
				dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId());

		return sample;
	}

	private InputStream populatingSample(ResultSet results) throws SQLException, DataSrcException {

		StringBuilder resultString = new StringBuilder();
		// populating metaddata
				ResultSetMetaData rsmd = results.getMetaData();
				int columnCount = rsmd.getColumnCount();

				// appending column names
				for (int i = 1; i <= columnCount; i++) {
					resultString.append(rsmd.getColumnName(i));
					resultString.append(",");
				}
				resultString.append("\r\n");

				// browsing through resultset
				while (results.next()) {
					for (int i = 1; i <= columnCount; i++) {
						if (i > 1) {
							resultString.append(",");
						}
						int type = rsmd.getColumnType(i);

						switch (type) {
						case (Types.VARCHAR):
						case (Types.CHAR):
						case (Types.LONGNVARCHAR):
							resultString.append(results.getString(i));
							break;
						case (Types.BIT):
							resultString.append(String.valueOf(results.getBoolean(i)));
							break;
						case (Types.BIGINT):
							resultString.append(String.valueOf(results.getLong(i)));
							break;
						case (Types.NUMERIC):
						case (Types.DECIMAL):
							resultString.append(String.valueOf(results.getBigDecimal(i)));
							break;
						case (Types.TINYINT):
						case (Types.SMALLINT):
						case (Types.INTEGER):
							resultString.append(String.valueOf(results.getInt(i)));
							break;
						case (Types.REAL):
							resultString.append(String.valueOf(results.getFloat(i)));
							break;
						case (Types.FLOAT):
						case (Types.DOUBLE):
							resultString.append(String.valueOf(results.getDouble(i)));
							break;
						case (Types.BINARY):
						case (Types.LONGVARBINARY):
						case (Types.VARBINARY):
							resultString.append(String.valueOf(results.getByte(i)));
							break;
						case (Types.DATE):
						case (Types.TIME):
						case (Types.TIMESTAMP):
							resultString.append(results.getTimestamp(i));
							break;
						default:
							DataSrcRestError err = DataSrcErrorList.buildError(new Exception("OOPS. Dev missed mapping for DATA Type.. Please raise a bug."), null, CmlpApplicationEnum.DATASOURCE);
							throw new DataSrcException("OOPS. Dev missed mapping. Please raise a bug.",
									Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
						}
					}
					resultString.append("\r\n");
				}

				return new ByteArrayInputStream(resultString.toString().getBytes());
	}
}
