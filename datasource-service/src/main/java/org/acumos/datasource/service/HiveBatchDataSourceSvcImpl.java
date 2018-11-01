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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Map;

import javax.ws.rs.core.Response.Status;

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
import org.acumos.datasource.schema.DataSourceMetadata;
import org.acumos.datasource.schema.DataSourceModelGet;
import org.acumos.datasource.schema.NameValue;
import org.acumos.datasource.utils.ApplicationUtilities;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


@Service
public class HiveBatchDataSourceSvcImpl implements HiveBatchDataSourceSvc {

	private static Logger log = LoggerFactory.getLogger(HiveBatchDataSourceSvcImpl.class);

	private Connection con;

	@Autowired
	private DbUtilitiesV2 dbUtilities;
	
	@Autowired
	HelperTool helperTool;
	
	@Autowired
	ApplicationUtilities applicationUtilities;

	public HiveBatchDataSourceSvcImpl() {
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

		applicationUtilities.createKerberosKeytab(objKerberosLogin);

		log.info("Created kerberos keytab for principal " + objKerberosLogin.getKerberosLoginUser());

	}

	@Override
	public String getConnectionStatusWithKerberos(KerberosLogin objKerberosLogin, String hostName, String port,
			String query) throws ClassNotFoundException, SQLException, IOException, DataSrcException {

		log.info("Creating kerberos keytab for hostname " + hostName + " using principal "
				+ objKerberosLogin.getKerberosLoginUser() + " for hive connectivity testing.");

		applicationUtilities.createKerberosKeytab(objKerberosLogin);

		log.info("Testing hive connectivity for hostname " + hostName + " using principal "
				+ objKerberosLogin.getKerberosLoginUser() + " after creating kerberos keytab");

		String result = getConnectionStatusMetadataWithKerberos(objKerberosLogin, hostName, port, query);

		log.info("test connectivity to hive resulted in " + result);
		return result;
	}

	@Override
	public Connection getConnection(String hostName, String kerberosLoginUser, String port, String kerberosRealm)
			throws IOException, ClassNotFoundException, SQLException, DataSrcException {
		log.info("Creating kerberised hadoop configuration for hostname " + hostName + " using principal "
				+ kerberosLoginUser + " for hive connectivity testing.");

		Configuration config = helperTool.getKerberisedConfiguration(hostName,
				kerberosLoginUser);

		log.info("loading JDBC driver class for hive connectivity");
		Class.forName("org.apache.hive.jdbc.HiveDriver");

		//EE3488
		String transportMode=helperTool.getEnv("hive_jdbc_transportMode", helperTool.getComponentPropertyValue("hive_jdbc_transportMode"));
		String httpPath=helperTool.getEnv("hive_jdbc_httpPath", helperTool.getComponentPropertyValue("hive_jdbc_httpPath"));
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
		}else
		{
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
	public InputStream getResults(String user, String authorization, String namespace, String datasourceKey, String batchSize)
			throws DataSrcException, IOException, ClassNotFoundException, SQLException {

		ArrayList<String> dbDatasourceDetails = dbUtilities.getDataSourceDetails(user, null, null, datasourceKey, null, true, false, authorization);
		DataSourceModelGet dbDataSource = applicationUtilities.getDataSourceModel(dbDatasourceDetails.get(0));
		
		if (dbDataSource.getCategory().equals("hive batch") && dbDataSource.getOwnedBy().equals(user)) {

			Map<String, String>  decryptionMap = applicationUtilities.readFromMongoCodeCloud(user, datasourceKey);
			
			StringBuilder resultString = null;
			
			//if dataReferenceBox is checked, return config file/keytab file contents
			if (dbDataSource.isDataReference()) {
				return getDataReference(dbDatasourceDetails.get(0), decryptionMap);
			}
			
			log.info("getResults(), restoring config files from codecloud...");
			String kerberosConfigFileName = (applicationUtilities.getKerberosFileName(user, dbDataSource.getHdfsHiveDetails().getKerberosConfigFileId()) + ".krb5.conf");
			String kerberosKeyTabFileName = (applicationUtilities.getKerberosFileName(user, dbDataSource.getHdfsHiveDetails().getKerberosKeyTabFileId()) + ".keytab");
			
			boolean isWritten = applicationUtilities.writeToKerberosFile(kerberosConfigFileName, decryptionMap.get("KerberosConfigFileContents".toLowerCase()));
			
			if(isWritten) {
				isWritten = applicationUtilities.writeToKerberosFile(kerberosKeyTabFileName, decryptionMap.get("KerberosKeyTabContent".toLowerCase()));
			}
			
			if(!isWritten) {
				//1. delete config files, if any
				applicationUtilities.deleteUserKerberosConfigFiles(kerberosConfigFileName, kerberosKeyTabFileName);
				
				//2. throw Exception
				DataSrcRestError err = DataSrcErrorList.buildError(new Exception("Unable to retrieve connection parameters from the codecloud."), null, CmlpApplicationEnum.DATASOURCE);
				throw new DataSrcException("Unable to retrieve connection parameters from the codecloud.",
						Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
			}
			
			//successfully Restored the files
			log.info("getResults(), Kerberos files were restored successfully...Proceed with check connection");
			
			KerberosConfigInfo kerberosConfig = applicationUtilities.getKerberosConfigInfo(kerberosConfigFileName,
					kerberosKeyTabFileName);

			con = getConnection(dbDataSource.getCommonDetails().getServerName(),
					decryptionMap.get("kerberosLoginUser".toLowerCase()), String.valueOf(dbDataSource.getCommonDetails().getPortNumber()),
					kerberosConfig.getKerberosRealms());
			
			//Remove semicolon
			dbDataSource.getHdfsHiveDetails().setQuery(applicationUtilities.trimSemicolonAtEnd(dbDataSource.getHdfsHiveDetails().getQuery()));
			
			//preparing statement and executing it
			Statement statement = con.createStatement();
			statement.setMaxRows(Integer.parseInt(batchSize));
			ResultSet results = statement.executeQuery(dbDataSource.getHdfsHiveDetails().getQuery());
			
			resultString = new StringBuilder();
			
			//populating metaddata
			ResultSetMetaData rsmd = results.getMetaData();
			int columnCount = rsmd.getColumnCount();
			
			//appending column names
			for (int i = 1; i <= columnCount; i++) {
				if (i > 1) {
					resultString.append(",");
				}
				resultString.append(rsmd.getColumnName(i));
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
				throw new DataSrcException("Unable to retrieve decryption files",
						Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
			}
			
			JSONObject objJson = new JSONObject(jsonStr);
			
			objJson.put("kerberosconfigfilecontents",
					StringEscapeUtils.unescapeHtml(decryptionMap.get("kerberosconfigfilecontents")).replaceAll("@@@",
							""));
			objJson.put("kerberoskeytabcontent", decryptionMap.get("kerberoskeytabcontent"));
			objJson.put("kerberosloginuser", decryptionMap.get("kerberosloginuser"));
			objJson.remove("kerberosConfigFileId");
			objJson.remove("kerberosKeyTabFileId");
			
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


	private String getConnectionStatusMetadataWithKerberos(KerberosLogin objKerberosLogin, String hostName, String port,
			 String query) throws ClassNotFoundException, SQLException, IOException, DataSrcException {
			
		String connectionStatus = "failed";
		System.out.println("query=" + query);
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
			throws DataSrcException, IOException, ClassNotFoundException, SQLException {

		ArrayList<String> dbDatasourceDetails = dbUtilities.getDataSourceDetails(user, null, null, datasourceKey, null, true, false, authorization);
		
		DataSourceModelGet dbDataSource = applicationUtilities.getDataSourceModel(dbDatasourceDetails.get(0));
		
		if (dbDataSource.getCategory().equals("hive batch") && dbDataSource.getOwnedBy().equals(user)) {

			Map<String, String> decryptionMap = applicationUtilities.readFromMongoCodeCloud(user, datasourceKey);
			
			StringBuilder resultString = null;
			
			log.info("getSampleResults(), restoring config files from codecloud...");
			String kerberosConfigFileName = (applicationUtilities.getKerberosFileName(user, dbDataSource.getHdfsHiveDetails().getKerberosConfigFileId()) + ".krb5.conf");
			String kerberosKeyTabFileName = (applicationUtilities.getKerberosFileName(user, dbDataSource.getHdfsHiveDetails().getKerberosKeyTabFileId()) + ".keytab");
			
			boolean isWritten = applicationUtilities.writeToKerberosFile(kerberosConfigFileName, decryptionMap.get("KerberosConfigFileContents".toLowerCase()));
			
			if(isWritten) {
				isWritten = applicationUtilities.writeToKerberosFile(kerberosKeyTabFileName, decryptionMap.get("KerberosKeyTabContent".toLowerCase()));
			}
			
			if(!isWritten) {
				//1. delete config files, if any
				applicationUtilities.deleteUserKerberosConfigFiles(kerberosConfigFileName, kerberosKeyTabFileName);
				
				//2. throw Exception
				DataSrcRestError err = DataSrcErrorList.buildError(new Exception("Unable to retrieve connection parameters from the codecloud."), null, CmlpApplicationEnum.DATASOURCE);
				throw new DataSrcException("Unable to retrieve connection parameters from the codecloud.",
						Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
			}
			
			//successfully Restored the files
			log.info("getSampleResults(), Kerberos files were restored successfully...Proceed with check connection");
			
			KerberosConfigInfo kerberosConfig = applicationUtilities.getKerberosConfigInfo(kerberosConfigFileName,
					kerberosKeyTabFileName);

			con = getConnection(dbDataSource.getCommonDetails().getServerName(),
					decryptionMap.get("kerberosLoginUser".toLowerCase()), String.valueOf(dbDataSource.getCommonDetails().getPortNumber()),

					kerberosConfig.getKerberosRealms());
			
			//connectivity test passed
			if("write".equalsIgnoreCase(dbDataSource.getReadWriteDescriptor())) {
				//validate connection call as there is no getSamples() allowed on write flag
				return (new  ByteArrayInputStream("Success".getBytes()));
			}
			
			//Remove semicolon
			dbDataSource.getHdfsHiveDetails().setQuery(applicationUtilities.trimSemicolonAtEnd(dbDataSource.getHdfsHiveDetails().getQuery()));
			
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
				if (i > 1) {
					resultString.append(",");
				}
				resultString.append(rsmd.getColumnName(i));
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
			applicationUtilities.deleteUserKerberosConfigFiles(kerberosConfigFileName, kerberosKeyTabFileName);
			
			return new ByteArrayInputStream(resultString.toString().getBytes());
		}

		//No information
		String[] variables = {"datasourceKey"};
		
		DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
		
		throw new DataSrcException("No Sample Results found for the given DatasourceKey.",
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

		int MIN_SAMPLING_SIZE; //default to 50
		String strMIN_SAMPLING_SIZE = helperTool.getEnv("minimum_sampling_size",
				helperTool.getComponentPropertyValue("minimum_sampling_size"));
		MIN_SAMPLING_SIZE = strMIN_SAMPLING_SIZE != null ? Integer.parseInt(strMIN_SAMPLING_SIZE) : 50;

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
	
	private long calculateHiveResultSizeBySampling(ResultSet results, int rowCount) throws SQLException, DataSrcException, IOException {
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
					} else {
						DataSrcRestError err = DataSrcErrorList.buildError(new Exception("OOPS. Dev missed mapping for DATA Type.. Please raise a bug."), null, CmlpApplicationEnum.DATASOURCE);
						throw new DataSrcException("OOPS. Dev missed mapping. Please raise a bug.",
								Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
					}
				} // for loop ends
			} // if expression ends
			resultString.append("\r\n");
			// log.info("rowNumber completed: " + rowNumber);
			rowNumber++;
		}
		log.info("Past Last Record: " + rowNumber);
		long size = 0;
		size = resultString.toString().getBytes().length * 1L;
		log.info("Size of 20% Dataset sampling : " + size + " bytes");
		return size * 5;
	}
}
