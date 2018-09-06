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
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response.Status;

import org.apache.http.client.ClientProtocolException;

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
import org.acumos.datasource.model.JdbcConnectionModel;
import org.acumos.datasource.schema.DataSourceModelGet;
import org.slf4j.Logger;

@Service
public class JdbcDataSourceSvcImpl implements JdbcDataSourceSvc {

	private static Logger log = LoggerFactory.getLogger(JdbcDataSourceSvcImpl.class);

	public JdbcDataSourceSvcImpl() {
	}

	@Autowired
	private DbUtilitiesV2 dbUtilities;

	@Override
	public String getConnectionStatus(JdbcConnectionModel objJdbcConnectionModel, String sqlStatement, String readWriteFlag)
			throws ClassNotFoundException, SQLException, IOException, CmlpDataSrcException {
		
		log.info("getConnectionStatus: getReadWriteFlag:  " + readWriteFlag);
		log.info("getConnectionStatus: sqlStatement:  " + sqlStatement);
		
		Connection dbConnection  = null;
		Statement statement = null;
		ResultSet results = null;
		
		String connectionStatus = "failed";
		
		try {
			// checking not null for required parameter
			if (objJdbcConnectionModel.getJdbcURL() != null 
					&& objJdbcConnectionModel.getUsername() != null
					&& objJdbcConnectionModel.getPassword() != null) {
				
				dbConnection = getConnection(objJdbcConnectionModel.getJdbcURL(), objJdbcConnectionModel.getUsername(),
						objJdbcConnectionModel.getPassword());

				if (sqlStatement != null) {
					if ("read".equalsIgnoreCase(readWriteFlag) || "readWrite".equalsIgnoreCase(readWriteFlag)) {
						statement = dbConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
								ResultSet.CONCUR_READ_ONLY);
						// statement.setMaxRows(5); //this cause error when connecting to mysql db
						results = statement.executeQuery(sqlStatement);

						if (results != null) {
							objJdbcConnectionModel.setMetaData(dbUtilities.getJdbcMetadata(results));
							dbConnection.close();
							connectionStatus = "success";
						}
					} else if ("write".equalsIgnoreCase(readWriteFlag)){
						try {
							sqlStatement = sqlStatement.replace('\'', '"');
							log.info("getConnectionStatus: updated sqlStatement:  " + sqlStatement);
							statement = dbConnection.createStatement();
							statement.executeUpdate(sqlStatement);
							
							//execute query to get table meta data
							int bIndex = sqlStatement.indexOf("INSERT INTO")+11;
							if(bIndex == -1) {
								log.info("getConnectionStatus: Invalid insert query parameter:  " + sqlStatement);
								String[] variables = { "dbQuery"};
								CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
								throw new CmlpDataSrcException("Invalid insert query parameter",
										Status.BAD_REQUEST.getStatusCode(), err);
							}
							
							int eIndex = sqlStatement.indexOf("(")-1;
							String tableName = sqlStatement.substring(bIndex, eIndex).trim();
							String query = "SELECT * from " + tableName + ";";

							Statement querySstatement = dbConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
							results = querySstatement.executeQuery(query);
							if (results != null) {
								objJdbcConnectionModel.setMetaData(dbUtilities.getJdbcMetadata(results));
							}
							return "success";
							
						} catch (SQLException e) {
							log.error("Failed to execute insert", e);
							Utilities.raiseConnectionFailedException("jdbc");
						}

					} else {
						log.info("getConnectionStatus: Invalid read/write parameter:  " + readWriteFlag);
						String[] variables = { "readWriteDescriptor"};
						CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
						
						throw new CmlpDataSrcException("Invalid readWriteDescriptor to support this transaction.",
								Status.BAD_REQUEST.getStatusCode(), err);
					}
				} else if (dbConnection != null) { //connection with no query
					dbConnection.close();
					connectionStatus = "success";
				} 
			} 
			
			if(connectionStatus.equals("failed")) {
				Utilities.raiseConnectionFailedException("jdbc");
			}
			
		} catch (CmlpDataSrcException e) {
			throw e;
			
		} catch (Exception e) {
			log.error("Failed to connect", e);
			return connectionStatus;
			
		} finally {

			if (statement != null) {
				statement.close();
			}

			if (dbConnection != null) {
				dbConnection.close();
			}
		}
		
		return connectionStatus;
	}
	
	@Override
	public InputStream getResults(String user, String authorization, String namespace, String datasourceKey)
			throws CmlpDataSrcException, IOException, SQLException, ClassNotFoundException {
		
		return (getResults( user, authorization, namespace, datasourceKey, 0));
	}

	@Override
	public Connection getConnection(String jdbcURL, String username, String password)
			throws ClassNotFoundException, IOException, SQLException, CmlpDataSrcException {
		// registering driver	  
		  String jdbc_driverName ="" ;
		  /*jdbc url has diff syntax for diff DB type
			e.g., jdbc:mysql://localhost:3306/mysqlDB
				  jdbc:oracle:thin:@localhost:1521:sid
		*/
		if (jdbcURL.contains("jdbc:mysql")){
			jdbc_driverName = "jdbc_mysql_driver_name";
		}
		else if (jdbcURL.contains("jdbc:oracle")) {
			jdbc_driverName = "jdbc_oracle_driver_name";
		}
		else {
			//throw new Exception("please provide a valid jdbc url");xxx
			String[] variables = {"jdbcURL"};
			CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
			
			throw new CmlpDataSrcException(
					"Please provide a valid jdbc url", Status.BAD_REQUEST.getStatusCode(), err);
		}
		Class.forName(HelperTool.getEnv(jdbc_driverName, HelperTool.getComponentPropertyValue(jdbc_driverName)));
		Connection connection = null;
	
		// preparing URL
		log.info("connection url for jdbc: " + jdbcURL);
		connection = DriverManager.getConnection(jdbcURL, username, password);
		return connection;
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

		List<String> dbDatasourceDetails = dbUtilities.getDataSourceDetails(user, null, null, datasourceKey, null, true, false, authorization);
		
		DataSourceModelGet dbDataSource = Utilities.getDataSourceModel(dbDatasourceDetails.get(0));
		if (dbDataSource.getReadWriteDescriptor().equalsIgnoreCase("write")){
			String[] variables = {"readWriteDescriptor"};
			CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
			throw new CmlpDataSrcException("Invalid Request.  Can not request contents for write datasource.",
					Status.BAD_REQUEST.getStatusCode(), err);
			
		}
		
		if (dbDataSource.getCategory().equals("jdbc") && dbDataSource.getOwnedBy().equals(user)) {
			Map<String, String> decrptionMap = new HashMap<>();
			decrptionMap = Utilities.readFromCodeCloud(authorization, datasourceKey);
			StringBuilder resultString = new StringBuilder();
			
			//creating connection
			Connection con = getConnection(dbDataSource.getDbDetails().getJdbcURL(), decrptionMap.get("dbServerUsername".toLowerCase()),
					decrptionMap.get("dbServerPassword".toLowerCase()));
			//preparing statement and executing it
			Statement statement = con.createStatement();
			statement.setMaxRows(rowsToReturn); //zero means no limit
			ResultSet results = statement.executeQuery(dbDataSource.getDbDetails().getDbQuery());
			
			//populating metadata
			resultString = populatingSample(results);
			
			return new ByteArrayInputStream(resultString.toString().getBytes());
		}
		
		//No information
		String[] variables = {"datasourceKey"};
		
		CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
		
		throw new CmlpDataSrcException("No Results found for the given DatasourceKey.",
				Status.NOT_FOUND.getStatusCode(), err);
	}
	
	private StringBuilder populatingSample(ResultSet results) throws SQLException, CmlpDataSrcException {
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
		
		//browsing through resultset
		while (results.next()) {
			for (int i = 1; i <= columnCount; i++) {
				if (i > 1) {
					resultString.append(",");
				}
				int type = rsmd.getColumnType(i);	
				if (type == Types.VARCHAR || type == Types.CHAR || type == Types.LONGNVARCHAR || type == Types.LONGVARCHAR) {
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
				} else if(type == Types.BLOB ) {
					resultString.append( String.valueOf(results.getBlob(i)) );
				} else {
					//throw new CmlpDataSrcException("OOPS. Dev missed mapping. Please raise a bug.");
					CmlpRestError err = CmlpErrorList.buildError(new Exception("OOPS. Dev missed mapping for DATA Type.. Please raise a bug."), null, CmlpApplicationEnum.DATASOURCE);
					throw new CmlpDataSrcException("OOPS. Dev missed mapping. Please raise a bug.",
							Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
				}
			}
			resultString.append("\r\n");
		}
		return resultString;
	}

	public long getJdbcDatasetSize(ResultSet results) throws SQLException, IOException, CmlpDataSrcException {
		log.info("getJdbcDatasetSize()");

		// final int DATASET_SAMPLE_SIZE = 50;
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
		// checking not null for required parameter
		Utilities.validateConnectionParameters(dataSource);

		int rowsLimit; //default to 5
		String strRowsLimit = HelperTool.getEnv("datasource_sample_size",
				HelperTool.getComponentPropertyValue("datasource_sample_size"));
		rowsLimit = strRowsLimit != null ? Integer.parseInt(strRowsLimit) : 5;
		
		StringBuilder resultString = new StringBuilder();
		
		//creating connection
		Connection con = getConnection(dataSource.getDbDetails().getJdbcURL(), dataSource.getDbDetails().getDbServerUsername(),
				dataSource.getDbDetails().getDbServerPassword());
		//preparing statement and executing it
		Statement statement = con.createStatement();
		statement.setMaxRows(rowsLimit); //zero means no limit
		ResultSet results = statement.executeQuery(dataSource.getDbDetails().getDbQuery());
		
		//populating sample
		resultString = populatingSample(results);
		
		return new ByteArrayInputStream(resultString.toString().getBytes());
	}
	
	
	@Override
	public boolean writebackPrediction(String authorization, DataSourceModelGet dataSource, String[] headerRow, List<String[]> content) 
			throws CmlpDataSrcException, IOException, ClassNotFoundException {
		log.info("writebackPrediction starting");

		Map<String, String> decrptionMap = new HashMap<>();
		decrptionMap = Utilities.readFromCodeCloud(authorization, dataSource.getDatasourceKey());
		
		try {
			//creating connection
			log.info("JdbcDataSourceSvcImpl.writebackPrediction:  url: " + dataSource.getDbDetails().getJdbcURL());
			log.info("JdbcDataSourceSvcImpl.writebackPrediction:  username: " + decrptionMap.get("dbServerUsername".toLowerCase()));
			log.info("JdbcDataSourceSvcImpl.writebackPrediction:  password: " + decrptionMap.get("dbServerPassword".toLowerCase()));
			Connection con = getConnection(dataSource.getDbDetails().getJdbcURL(), decrptionMap.get("dbServerUsername".toLowerCase()),
					decrptionMap.get("dbServerPassword".toLowerCase()));
				
			//preparing statement and executing it
			String dbInsertQuery = dataSource.getDbDetails().getDbQuery();
			log.info("JdbcDataSourceSvcImpl.writebackPrediction:  originalquery: " + dbInsertQuery);
			
			int indexx = dbInsertQuery.indexOf("VALUES") + 6;
			String dbInsertSubstring = dbInsertQuery.substring(0, indexx);
			String dbInsert = "";
			
			Statement statement = con.createStatement();
				
            if (content != null && content.size() > 0) {
            	String insertValues = "";
            	String singleValue = "";
            	String column = "";
                for (String[] row : content) {
            		insertValues = "";
                	for ( int i=0; i<row.length; i++) {
                		column = row[i];
                		if(i == 0) {
                			//singleValue = "'" + column + "'";
                			singleValue =  column ;
                		}else {
                			//singleValue = ", '" + column + "'";
                			singleValue = ", " + column;
                		}
                		insertValues = insertValues + singleValue;
                	}
                    dbInsert = dbInsertSubstring + " (" + insertValues + ");";
            			log.info("JdbcDataSourceSvcImpl.writebackPrediction:  dbInsert: " + dbInsert);
            			statement.executeUpdate(dbInsert);
                    }
            }else {
    			log.error("JdbcDataSourceSvcImpl.writebackPrediction: There is no content to write.  Datafile must be csv file.");
    			
    			String[] variables = { "dataFile"};
    			CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum._0004, variables, null, CmlpApplicationEnum.DATASOURCE); 			
    			throw new CmlpDataSrcException("Failed to execute insert.  There is no content to write", Status.BAD_REQUEST.getStatusCode(), err);
	            }
				

		} catch (SQLException e) {
			log.error("JdbcDataSourceSvcImpl.writebackPrediction: Failed to execute insert", e);
			String[] variables = { "dbQuery"};
			CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum._0001, variables, null, CmlpApplicationEnum.DATASOURCE);
			throw new CmlpDataSrcException("Failed to execute insert", Status.INTERNAL_SERVER_ERROR  .getStatusCode(), err);
		}
		
		return true;
	}

}

