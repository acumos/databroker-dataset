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
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.Response.Status;

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
import org.acumos.datasource.model.MysqlConnectorModel;
import org.acumos.datasource.schema.ColumnMetadataInfo;
import org.acumos.datasource.schema.DataSourceModelGet;
import org.acumos.datasource.schema.DataSourceMetadata;
import org.acumos.datasource.schema.NameValue;
import org.slf4j.Logger;

@Service
public class MySqlDataSourceSvcImpl implements MySqlDataSourceSvc {

	private static Logger log = LoggerFactory.getLogger(MySqlDataSourceSvcImpl.class);

	public MySqlDataSourceSvcImpl() {
	}

	@Autowired
	private DbUtilitiesV2 dbUtilities;

	@Override
	public String getConnectionStatus(MysqlConnectorModel objMysqlConnectorModel, String query)
			throws ClassNotFoundException, SQLException, IOException, CmlpDataSrcException {
		log.info("getConnectionStatus(), checking required parameters for identifying null values");
		//final long MAX_DATASET_SIZE = 1024 * 1024 * 1024 * 4;
		// checking not null for required parameter
		if (objMysqlConnectorModel.getHostname() != null && objMysqlConnectorModel.getPort() != null
				&& objMysqlConnectorModel.getDbName() != null && objMysqlConnectorModel.getUserName() != null
				&& objMysqlConnectorModel.getPassword() != null) {

			Connection con = getConnection(objMysqlConnectorModel.getHostname(), objMysqlConnectorModel.getPort(),
					objMysqlConnectorModel.getDbName(), objMysqlConnectorModel.getUserName(),
					objMysqlConnectorModel.getPassword());
			// checking query
			//query = "select * from cmlp.authors;";
			if (query != null) {
				System.out.println("\nThe SQL Query is " + query );

				Statement statement = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				ResultSet results = null;
				results = statement.executeQuery(query);
				/*long datasetSize = getJdbcDatasetSize(results);
			
				double size1= datasetSize/1.0;
				String unit = " B";
				
				if (datasetSize > 1024 * 1024) {
					size1 = datasetSize/(1024*1024.0);
					unit = " MB";
				} else if (datasetSize > 1024) {
					size1 = datasetSize/1024.0;
					unit = " KB";
				}
				
				System.out.println("The dataset size is " + size1 + unit);
				System.out.println();
				System.out.println();
*/
				
				if (results != null) {
					objMysqlConnectorModel.setMetaData(getMySqlMetadata(results));
					con.close();
					return "success";
				} else {
					con.close();
					return "failed";
				} // end if
				
			} else if (con != null) {
				con.close();
				return "success";
			}
		}
		
		return "failed";
	}

	@Override
	public Connection getConnection(String server, String port, String dbName, String username, String password)
			throws ClassNotFoundException, IOException, SQLException, CmlpDataSrcException {
		// registering driver
		Class.forName(
				HelperTool.getEnv("mysql_driver_name", HelperTool.getComponentPropertyValue("mysql_driver_name")));
		Connection connection = null;

		// preparing URL
		String dbUrl = "jdbc:mysql://" + server + ":" + port + "/" + dbName;
		log.info("connection url for mysql: " + dbUrl);
		connection = DriverManager.getConnection(dbUrl, username, password);
		return connection;
	}

	@Override
	public InputStream getResults(String user, String authorization, String namespace, String datasourceKey)
			throws CmlpDataSrcException, IOException, SQLException, ClassNotFoundException {
		
		ArrayList<String> dbDatasourceDetails = dbUtilities.getDataSourceDetails(user, null, null, datasourceKey, null, true, false,authorization);
		
		DataSourceModelGet dbDataSource = Utilities.getDataSourceModel(dbDatasourceDetails.get(0));
		
		if (dbDataSource.getCategory().equals("mysql") && dbDataSource.getOwnedBy().equals(user)) {
			Map<String, String> decryptionMap = new HashMap<>();
			//decryptionMap = Utilities.readFromCodeCloud(authorization, datasourceKey);
			decryptionMap = Utilities.readFromMongoCodeCloud(user, datasourceKey);
			
			StringBuilder resultString = null;
			
			//creating connection
			Connection con = getConnection(dbDataSource.getCommonDetails().getServerName(), String.valueOf(dbDataSource.getCommonDetails().getPortNumber()),
					dbDataSource.getDbDetails().getDatabaseName(), decryptionMap.get("dbServerUsername".toLowerCase()),
					decryptionMap.get("dbServerPassword".toLowerCase()));
			
			//preparing statement and executing it
			Statement statement = con.createStatement();
			ResultSet results = statement.executeQuery(dbDataSource.getDbDetails().getDbQuery());
			
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
				//resultString.append(",");
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
						resultString.append(String.valueOf(results.getTime(i)));
					} else {
						//throw new CmlpDataSrcException("OOPS. Dev missed mapping. Please raise a bug.");
						CmlpRestError err = CmlpErrorList.buildError(new Exception("OOPS. Dev missed mapping for DATA Type.. Please raise a bug."), null, CmlpApplicationEnum.DATASOURCE);
						throw new CmlpDataSrcException("OOPS. Dev missed mapping. Please raise a bug.",
								Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
					}
				}
				resultString.append("\r\n");
			}
			return new ByteArrayInputStream(resultString.toString().getBytes());
		} 
		
		//No information
		String[] variables = {"datasourceKey"};
		
		CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
		
		throw new CmlpDataSrcException("No Results found for the given DatasourceKey.",
				Status.NOT_FOUND.getStatusCode(), err);
	}

	@Override
	public InputStream getSampleResults(String user, String authorization, String namespace, String datasourceKey)
			throws CmlpDataSrcException, IOException, SQLException, ClassNotFoundException {
		
		int rowsLimit; //default to 5
		String strRowsLimit = HelperTool.getEnv("datasource_sample_size",
				HelperTool.getComponentPropertyValue("datasource_sample_size"));
		rowsLimit = strRowsLimit != null ? Integer.parseInt(strRowsLimit) : 5;

		return (getResults( user, authorization, namespace, datasourceKey, rowsLimit));

	}
	
	
	private InputStream getResults(String user, String authorization, String namespace, String datasourceKey, int rowsToReturn)
			throws CmlpDataSrcException, IOException, SQLException, ClassNotFoundException {

		ArrayList<String> dbDatasourceDetails = dbUtilities.getDataSourceDetails(user, null, null, datasourceKey, null, true, false, authorization);
		//JSONObject objJson = new JSONObject(cassandraDetails.get(0));
		
		DataSourceModelGet dbDataSource = Utilities.getDataSourceModel(dbDatasourceDetails.get(0));
		
		if (dbDataSource.getCategory().equals("mysql") && dbDataSource.getOwnedBy().equals(user)) {
			Map<String, String> decryptionMap = new HashMap<>();
			//decryptionMap = Utilities.readFromCodeCloud(authorization, datasourceKey);
			decryptionMap = Utilities.readFromMongoCodeCloud(user, datasourceKey);
			
			StringBuilder resultString = null;
			
			//creating connection
			Connection con = getConnection(dbDataSource.getCommonDetails().getServerName(), String.valueOf(dbDataSource.getCommonDetails().getPortNumber()),
					dbDataSource.getDbDetails().getDatabaseName(), decryptionMap.get("dbServerUsername".toLowerCase()),
					decryptionMap.get("dbServerPassword".toLowerCase()));
			
			//preparing statement and executing it
			Statement statement = con.createStatement();
			//statement.setFetchSize(rowsToReturn);// no effect
			//does not work for mysql for mysql-connector-java 5.1.6(May, 2015) in order to work the need upgrade as min 5.1.20(May, 2015). 
			//Was determined to upgrade to 5.1.45(Nov, 2017) which is the latest within 5.1.x. 
			statement.setMaxRows(rowsToReturn);
			ResultSet results = statement.executeQuery(dbDataSource.getDbDetails().getDbQuery());
			
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
				//resultString.append(",");
			}
			resultString.append("\r\n");
			
			//browsing through resultset for limited rowsToReturn  since setMaxRows is not working for mysql. 
			//This is not the ideal solution until there is another better solution, upgrade  mysql-connector-java 5.1.6(May, 2015) to 5.1.45(Nov, 2017)
			//while (  rowsToReturn == 0 &&  results.next() || results.next() && results.getRow()<=rowsToReturn ) {
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
						//throw new CmlpDataSrcException("OOPS. Dev missed mapping. Please raise a bug.");
						CmlpRestError err = CmlpErrorList.buildError(new Exception("OOPS. Dev missed mapping for DATA Type.. Please raise a bug."), null, CmlpApplicationEnum.DATASOURCE);
						throw new CmlpDataSrcException("OOPS. Dev missed mapping. Please raise a bug.",
								Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
					}
				}
				resultString.append("\r\n");
			}
			return new ByteArrayInputStream(resultString.toString().getBytes());
		} 
			
		//No information
		String[] variables = {"datasourceKey"};
		
		CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
		
		throw new CmlpDataSrcException("No Results found for the given DatasourceKey.",
				Status.NOT_FOUND.getStatusCode(), err);
	}
	
	public DataSourceMetadata getMySqlMetadata(ResultSet results) throws SQLException, IOException, CmlpDataSrcException {
		log.info("getMySqlMetadata()");
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
		
		long datasetSize = getMySqlDatasetSize(results);
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
		metadata.setMetaDataInfo(lstSimpleMetadata);
		metadata.setColumnMetaDataInfo(lstcolumnMetadata);
		System.out.println("MySql Metadata: \n" + metadata);
		return metadata;
	}
	
	public long getMySqlDatasetSize(ResultSet results) throws SQLException, IOException, CmlpDataSrcException {
		log.info("getMySqlDatasetSize()");
		//final int DATASET_SAMPLE_SIZE = 50;
		int MIN_SAMPLING_SIZE; //default to 50
		String strMIN_SAMPLING_SIZE = HelperTool.getEnv("minimum_sampling_size",
				HelperTool.getComponentPropertyValue("minimum_sampling_size"));
		MIN_SAMPLING_SIZE = strMIN_SAMPLING_SIZE != null ? Integer.parseInt(strMIN_SAMPLING_SIZE) : 50;

		long size = 0;
		
		results.last();
		int rowCount = results.getRow();
		results.beforeFirst();
		
		if (rowCount <= MIN_SAMPLING_SIZE) { 
			size = new HelperTool().calculateJdbcResultSizeByNotSampling(results);
		} else {
			size = new HelperTool().calculateJdbcResultSizeBySampling(results);
		}
		
		return size;
	}

	@Override
	public InputStream getSampleResults(DataSourceModelGet dataSource) throws IOException, ClassNotFoundException, SQLException, CmlpDataSrcException {
		log.info("getSampleResultsBeforeRegistration call...");
		
		if(dataSource.getDbDetails() != null) {
			if(dataSource.getDbDetails().getDbQuery() == null || dataSource.getDbDetails().getDbQuery().isEmpty()) {
				String defaultQuery = HelperTool.getEnv("test_mysql_query",
						HelperTool.getComponentPropertyValue("test_mysql_query"));
				dataSource.getDbDetails().setDbQuery(defaultQuery);
			}
			
		}
		
		//check for required connection parameters
		Utilities.validateConnectionParameters(dataSource);
		
		int rowsLimit; //default to 5
		String strRowsLimit = HelperTool.getEnv("datasource_sample_size",
				HelperTool.getComponentPropertyValue("datasource_sample_size"));
		rowsLimit = strRowsLimit != null ? Integer.parseInt(strRowsLimit) : 5;
		
		//creating connection
		Connection con = getConnection(dataSource.getCommonDetails().getServerName(), String.valueOf(dataSource.getCommonDetails().getPortNumber()),
				dataSource.getDbDetails().getDatabaseName(), dataSource.getDbDetails().getDbServerUsername(),
				dataSource.getDbDetails().getDbServerPassword());
		
		//preparing statement and executing it
		Statement statement = con.createStatement();
		//statement.setFetchSize(rowsToReturn);// no effect
		//does not work for mysql for mysql-connector-java 5.1.6(May, 2015) in order to work the need upgrade as min 5.1.20(May, 2015). 
		//Was determined to upgrade to 5.1.45(Nov, 2017) which is the latest within 5.1.x. 
		statement.setMaxRows(rowsLimit);
		
		ResultSet results = statement.executeQuery(dataSource.getDbDetails().getDbQuery());	
		
		return populatingSample(results);

	}

	private InputStream populatingSample(ResultSet results) throws SQLException, CmlpDataSrcException {
		StringBuilder resultString = new StringBuilder();
		
		ResultSetMetaData rsmd = results.getMetaData();
		int columnCount = rsmd.getColumnCount();
		
		//appending column names
		for (int i = 1; i <= columnCount; i++) {
			if (i > 1) {
				resultString.append(",");
			}
			resultString.append(rsmd.getColumnName(i));
			//resultString.append(",");
		}
		resultString.append("\r\n");
		
		//browsing through resultset for limited rowsToReturn  since setMaxRows is not working for mysql. 
		//This is not the ideal solution until there is another better solution, upgrade  mysql-connector-java 5.1.6(May, 2015) to 5.1.45(Nov, 2017)
		//while (  rowsToReturn == 0 &&  results.next() || results.next() && results.getRow()<=rowsToReturn ) {
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
					//throw new CmlpDataSrcException("OOPS. Dev missed mapping. Please raise a bug.");
					CmlpRestError err = CmlpErrorList.buildError(new Exception("OOPS. Dev missed mapping for DATA Type.. Please raise a bug."), null, CmlpApplicationEnum.DATASOURCE);
					throw new CmlpDataSrcException("OOPS. Dev missed mapping. Please raise a bug.",
							Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
				}
			}
			resultString.append("\r\n");
		}
		return new ByteArrayInputStream(resultString.toString().getBytes());
	}
	
}
