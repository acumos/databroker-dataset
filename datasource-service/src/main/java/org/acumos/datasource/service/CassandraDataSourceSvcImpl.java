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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.ws.rs.core.Response.Status;

import org.acumos.datasource.common.CmlpApplicationEnum;
import org.acumos.datasource.common.DataSrcErrorList;
import org.acumos.datasource.common.DataSrcRestError;
import org.acumos.datasource.common.ErrorListEnum;
import org.acumos.datasource.common.HelperTool;
import org.acumos.datasource.connection.DbUtilitiesV2;
import org.acumos.datasource.exception.DataSrcException;
import org.acumos.datasource.model.CassandraConnectionModel;
import org.acumos.datasource.schema.ColumnMetadataInfo;
import org.acumos.datasource.schema.DataSourceMetadata;
import org.acumos.datasource.schema.DataSourceModelGet;
import org.acumos.datasource.schema.NameValue;
import org.acumos.datasource.utils.ApplicationUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;


@Service
public class CassandraDataSourceSvcImpl implements CassandraDataSourceSvc {

	private static Logger log = LoggerFactory.getLogger(CassandraDataSourceSvcImpl.class);

	private Session session;
	private int DEFAULT_SAMPLE_SIZE = 5;

	public CassandraDataSourceSvcImpl() {
	}

	@Autowired
	private DbUtilitiesV2 dbUtilities;
	
	@Autowired
	HelperTool helperTool;
	
	@Autowired
	ApplicationUtilities applicationUtilities;

	@Override
	public String getConnectionStatus(CassandraConnectionModel objCassandraConnectionModel, String query)
			throws DataSrcException, IOException, SQLException {
		log.info("ENTER:getConnectionStatus(), checking required parameters for identifying null values");
		
		String connectionMessage = "failed";
		
		if (objCassandraConnectionModel.getNode() != null && objCassandraConnectionModel.getPassword() != null
				&& objCassandraConnectionModel.getPort() != 0 && objCassandraConnectionModel.getUserName() != null) {
			log.info("getConnectionStatus(), using credentials, initializing required variables");

			ResultSet results = null;
			
			int rowCount=0;
			log.info("getConnectionStatus(), using credetials, initializing cassandra session");
			Session session = getSession(objCassandraConnectionModel.getNode(), objCassandraConnectionModel.getPort(),
					objCassandraConnectionModel.getUserName(), objCassandraConnectionModel.getPassword());
			log.info("getConnectionStatus(), using credetials, acquired cassandra session");

			if (session != null && query == null) {
				connectionMessage = "success";
			} else if (session != null && query != null) {

				log.info("query: " + query);

				results = session.execute(query);
				rowCount = getRowCount(results);
				//getRowCount takes the cursor past the last record
				// results.isExhausted returns true
				log.info("rowCount: " + rowCount);
				//Second Call necessary to reset the ResultSet to the Beginning
				results = session.execute(query);
				connectionMessage = !(results.isExhausted()) ? "success" : "failed";
				log.info("connectionMessage: " + connectionMessage);
				DataSourceMetadata metaData = getCassandraMetadata(results, rowCount);
				objCassandraConnectionModel.setMetaData(metaData);
				connectionMessage = "success";
			} else {
				log.info("Something went wrong while session initialization or running query");
				raiseInvalidInputParameters();
			}
			log.info("getConnectionStatus, using credetials, closing cassandra session");
			session.close();
			log.info("connectionMessage: " + connectionMessage);
		} else {
			log.info("Please check input parameters");
			raiseInvalidInputParameters();
		}
		
		return connectionMessage;
	}

	@Override
	public String getConnectionStatus(String node, int port, String keyspaceName) throws DataSrcException {

		String connectionMessage = "failed";
		
		if (node != null && port != 0) {
			log.info("getConnectionStatus, without credetials, initializing cassandra connection");

			Cluster cluster = null;
			Session session = null;

			log.info("getConnectionStatus, without credetials, initializing cassandra cluster");
			cluster = Cluster.builder().addContactPoint(node).withPort(port).build();
			Metadata metadata = cluster.getMetadata();
			log.info("getConnectionStatus, Connected to cassandra cluster: " + metadata.getClusterName());

			log.info("getConnectionStatus, without credetials, initializing cassandra session");
			session = cluster.connect();
			if (session != null) {
				connectionMessage = "success";
			}

			if (keyspaceName != null) {
				log.info("getConnectionStatus, without credetials, checking keyspace");
				createkeySpace(session, keyspaceName, "SimpleStrategy", 1);
			}

			log.info("getConnectionStatus, without credetials, closing cassandra session");
			session.close();
			return connectionMessage;
		} else {
			raiseInvalidInputParameters();
		}

		return connectionMessage;
	}

	@Override
	public void createkeySpace(Session session, String keySpaceName, String replicationStrategy,
			int replicationFactor) throws DataSrcException {

		log.info("createkeySpace, building string query to create if keyspace doesn't exist for keyspace: "
				+ keySpaceName);
		StringBuilder sb = new StringBuilder("CREATE KEYSPACE IF NOT EXISTS cmlp_datasrc_test ")
				.append(" WITH replication = {").append("'class':'").append(replicationStrategy)
				.append("','replication_factor':").append(replicationFactor).append("};");

		String query = sb.toString();

		log.info("createkeySpace, executing query for keyspace: " + keySpaceName);
		session.execute(query);
	}

	@Override
	public Session getSession(String node, int port, String username, String password) throws DataSrcException {
		log.info("getSession, using credetials");

		log.info("getSession, node: " + node);
		log.info("getSession, port: " + port);
		log.info("getSession, username: " + username);

		Cluster cluster = null;
		Session session = null;

		log.info("getSession, using credetials, initializing cassandra cluster");
		cluster = Cluster.builder().addContactPoint(node).withPort(port).withCredentials(username, password).build();
		Metadata metadata = cluster.getMetadata();

		log.info("getSession, Connected to cassandra cluster: " + metadata.getClusterName());

		log.info("getSession, using credetials, initializing cassandra session");
		session = cluster.connect();
		return session;
	}

	@Override
	public InputStream getResults(String user, String authorization, String namespace, String datasourceKey)
			throws DataSrcException, IOException {

		ArrayList<String> dbDatasourceDetails = dbUtilities.getDataSourceDetails(user, null, null, datasourceKey, null, true, false, authorization);
		DataSourceModelGet dbDataSource = applicationUtilities.getDataSourceModel(dbDatasourceDetails.get(0));
		
		if (dbDataSource.getCategory().equals("cassandra") && dbDataSource.getOwnedBy().equals(user)) {
			ResultSet results = null;
			StringBuilder builderObject = new StringBuilder();
			Map<String, String> decryptionMap = new HashMap<>();

			decryptionMap = applicationUtilities.readFromMongoCodeCloud(user, datasourceKey);
					
			log.info("getResults, using credetials, initializing cassandra cluster");
			session = getSession(dbDataSource.getCommonDetails().getServerName(), dbDataSource.getCommonDetails().getPortNumber(),
					decryptionMap.get("dbServerUsername".toLowerCase()),
					decryptionMap.get("dbServerPassword".toLowerCase()));

			// executing query and getting no of resultset
			results = session.execute(dbDataSource.getDbDetails().getDbQuery());
			int columnCount = results.getColumnDefinitions().size();

			for (int i = 0; i < columnCount; i++) {
				if (i > 0) {
					builderObject.append(",");
				}
				builderObject.append(results.getColumnDefinitions().getName(i));
			}

			builderObject.append("\r\n");

			// parsing through the resultset
			for (Row row : results) {
				for (int i = 0; i < columnCount; i++) {
					if (i > 0) {
						builderObject.append(",");
					}

					// getting Java type for cassandra type
					String columnType = row.getColumnDefinitions().getType(i).getName().name();

					if (columnType.equals("ASCII") || columnType.equals("BLOB") || columnType.equals("INET")
							|| columnType.equals("TEXT") || columnType.equals("TIMEUUID")
							|| columnType.equals("VARCHAR") || columnType.equals("TUPLE")) {
						builderObject.append(row.getString(i));
					} else if (columnType.equals("BIGINT") || columnType.equals("BOOLEAN")
							|| columnType.equals("COUNTER")) {
						builderObject.append(row.getLong(i));
					} else if (columnType.equals("DATE")) {
						builderObject.append(row.getDate(i));
					} else if (columnType.equals("DECIMAL")) {
						builderObject.append(row.getDecimal(i));
					} else if (columnType.equals("DOUBLE") || columnType.equals("FLOAT")) {
						builderObject.append(row.getDouble(i));
					} else if (columnType.equals("INT")) {
						builderObject.append(row.getInt(i));
					} else if (columnType.equals("SMALLINT")) {
						builderObject.append(row.getShort(i));
					} else if (columnType.equals("TINYINT")) {
						builderObject.append(row.getByte(i));
					} else if (columnType.equals("TIME")) {
						builderObject.append(row.getTime(i));
					} else if (columnType.equals("TIMESTAMP")) {
						builderObject.append(row.getTimestamp(i));
					} else if (columnType.equals("UUID")) {
						builderObject.append(row.getUUID(i));
					} else {
						DataSrcRestError err = DataSrcErrorList.buildError(new Exception("OOPS. Dev missed mapping for DATA Type.. Please raise a bug."), null, CmlpApplicationEnum.DATASOURCE);
						throw new DataSrcException("OOPS. Dev missed mapping. Please raise a bug.",
								Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
					}
				}
				System.out.println("Printing rows" + row.toString());
				builderObject.append("\r\n");
			}

			log.info("getResults, using credetials, closing cassandra session");
			session.close();
			return new ByteArrayInputStream(builderObject.toString().getBytes());
		} 
		
		//No information
		String[] variables = {"datasourceKey"};
		DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
		
		throw new DataSrcException(
				"please check datasource key provided and user permission for this operation", Status.NOT_FOUND.getStatusCode(), err);
	}
	
	@Override
	public DataSourceMetadata getMetadataResults (
			String user, String authorization, String datasourceKey)
			throws DataSrcException, IOException, SQLException {

		ArrayList<String> dbDatasourceDetails = dbUtilities.getDataSourceDetails(user, null, null, datasourceKey, null, true, false, authorization);
		
		DataSourceModelGet dbDataSource = applicationUtilities.getDataSourceModel(dbDatasourceDetails.get(0));

		if (dbDataSource.getCategory().equals("cassandra") && dbDataSource.getOwnedBy().equals(user)) {
			ResultSet results = null;
			Map<String, String> decryptionMap = new HashMap<>();
			
			decryptionMap = applicationUtilities.readFromMongoCodeCloud(user, datasourceKey);

			log.info("getMetadataResults: using credetials, initializing cassandra cluster");

			session = getSession(dbDataSource.getCommonDetails().getServerName(), 
					dbDataSource.getCommonDetails().getPortNumber(),
					decryptionMap.get("dbServerUsername".toLowerCase()),
					decryptionMap.get("dbServerPassword".toLowerCase()));

			String query = dbDataSource.getDbDetails().getDbQuery();
			
			results = session.execute(query);
			int rowCount = getRowCount(results);
			log.info("getMetadataResults using credetials, closing cassandra session");
			session.close();
			return getCassandraMetadata(results, rowCount);
		} 

		String[] variables = {"datasourceKey"};
		DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
		
		throw new DataSrcException(
				"please check datasource key provided and user permission for this operation", Status.NOT_FOUND.getStatusCode(), err);

	}
	
	private DataSourceMetadata getCassandraMetadata(ResultSet results, int rowCount) throws DataSrcException, IOException, SQLException {
		log.info("getCassandraMetadata()");
		
		long datasetSize = getCassandraDatasetSize(results, rowCount);
		
		String value = String.valueOf(datasetSize) + " bytes";
		if (datasetSize == -1L) {
			value = "Not Available Due to BLOB columntype";
		}

		ColumnDefinitions clmdfn = results.getColumnDefinitions();
		int columnCount = clmdfn.size();
		
		//getting metaData
		ArrayList<NameValue> lstSimpleMetadata = new ArrayList<NameValue>();
		lstSimpleMetadata.add(new NameValue("columnCount", String.valueOf(columnCount)));
		lstSimpleMetadata.add(new NameValue("rowCount", String.valueOf(rowCount)));
		lstSimpleMetadata.add(new NameValue("dataSize", value));
		
		DataSourceMetadata metadata = new DataSourceMetadata();

		ArrayList<ColumnMetadataInfo> lstcolumnMetadata = new ArrayList<ColumnMetadataInfo>();
		for (int i = 0; i < columnCount; i++) {
			String name = clmdfn.getName(i);
			String type = clmdfn.getType(i).getName().name();
			lstcolumnMetadata.add(new ColumnMetadataInfo(name, type, "NA"));
		}
		
		metadata.setMetaDataInfo(lstSimpleMetadata);
		metadata.setColumnMetaDataInfo(lstcolumnMetadata);
		System.out.println("Cassandra Metadata: \n" + metadata);
		return metadata;	
	}

	@Override
	public InputStream getSampleResults(String user, String authorization, String namespace, String datasourceKey)
			throws DataSrcException, IOException {
		log.info( "ENTER:CassandraDataSourceSvcImpl::getSampleResults");
		int rowsLimit; //default to 5
		String strRowsLimit = helperTool.getEnv("datasource_sample_size", helperTool.getComponentPropertyValue("datasource_sample_size"));
		rowsLimit = strRowsLimit != null ? Integer.parseInt(strRowsLimit) : 5;

		log.info( "RETURN:CassandraDataSourceSvcImpl::getSampleResults");
		return getDatasourceSampleData(user, authorization, datasourceKey, rowsLimit);

	}


	private InputStream getDatasourceSampleData(String user, String authorization, String datasourceKey, int sampleSize)
			throws DataSrcException, IOException {
		log.info( "ENTER:CassandraDataSourceSvcImpl::getDatasetSampleData");

		ArrayList<String> dbDatasourceDetails = dbUtilities.getDataSourceDetails(user, null, null, datasourceKey, null, true, false, authorization);
		
		DataSourceModelGet dbDataSource = applicationUtilities.getDataSourceModel(dbDatasourceDetails.get(0));
		
		if (dbDataSource.getCategory().equals("cassandra")
				&& dbDataSource.getOwnedBy().equals(user)) {
			log.info( "CassandraDataSourceSvcImpl::getDatasetSampleData:category=cassandara");
			ResultSet results = null;
			StringBuilder builderObject = new StringBuilder();
			Map<String, String> decryptionMap = new HashMap<>();

			decryptionMap = applicationUtilities.readFromMongoCodeCloud(user, datasourceKey);

			log.info("\nCassandraDataSourceSvcImpl::getDatasetSampleData, using credetials, initializing cassandra cluster");
			session = getSession(dbDataSource.getCommonDetails().getServerName(), 
					dbDataSource.getCommonDetails().getPortNumber(),
					decryptionMap.get("dbServerUsername".toLowerCase()),
					decryptionMap.get("dbServerPassword".toLowerCase()));

			String query = dbDataSource.getDbDetails().getDbQuery();

			results = session.execute(query);
			
			builderObject = buildSampleData(results, sampleSize);
			
			log.info( "\nCassandraDataSourceSvcImpl::getDatasetSampleData OUTPUT:\r\n\n" + builderObject);
			
			log.info("CassandraDataSourceSvcImpl::getDatasetSampleData, using credetials, closing cassandra session");
			session.close();
			log.info( "RETURN:CassandraDataSourceSvcImpl::getDatasetSampleData");
			return new ByteArrayInputStream(builderObject.toString().getBytes());
		} else {
			String[] variables = {"datasourceKey"};
			DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
			
			throw new DataSrcException(
					"please check datasource key provided and user permission for this operation", Status.NOT_FOUND.getStatusCode(), err);
		}
	}

	private StringBuilder buildSampleData(ResultSet results, int sampleSize) throws DataSrcException {
		log.info( "ENTER:CassandraDataSourceSvcImpl::buildSampleData");

		StringBuilder builderObject = new StringBuilder();

		int columnCount = results.getColumnDefinitions().size();

		for (int i = 0; i < columnCount; i++) {
			if (i > 0) {
				builderObject.append(",");
			}
			builderObject.append(results.getColumnDefinitions().getName(i));
		}

		builderObject.append("\r\n");

		// parsing through the resultset
		
		for (int i =  0; i < sampleSize && !results.isExhausted(); i++) {
			Row row = results.one();

			for (int j = 0; j < columnCount; j++) {
				if (j > 0) {
					builderObject.append(",");
				}

				// getting Java type for cassandra type
				String columnType = row.getColumnDefinitions().getType(j).getName().name();

				if (columnType.equals("ASCII") || columnType.equals("BLOB") || columnType.equals("INET")
						|| columnType.equals("TEXT") || columnType.equals("TIMEUUID") || columnType.equals("VARCHAR")
						|| columnType.equals("TUPLE")) {
					builderObject.append(row.getString(j));
				} else if (columnType.equals("BIGINT") || columnType.equals("BOOLEAN")
						|| columnType.equals("COUNTER")) {
					builderObject.append(row.getLong(j));
				} else if (columnType.equals("DATE")) {
					builderObject.append(row.getDate(j));
				} else if (columnType.equals("DECIMAL")) {
					builderObject.append(row.getDecimal(j));
				} else if (columnType.equals("DOUBLE") || columnType.equals("FLOAT")) {
					builderObject.append(row.getDouble(j));
				} else if (columnType.equals("INT")) {
					builderObject.append(row.getInt(j));
				} else if (columnType.equals("SMALLINT")) {
					builderObject.append(row.getShort(j));
				} else if (columnType.equals("TINYINT")) {
					builderObject.append(row.getByte(j));
				} else if (columnType.equals("TIME")) {
					builderObject.append(row.getTime(j));
				} else if (columnType.equals("TIMESTAMP")) {
					builderObject.append(row.getTimestamp(j));
				} else if (columnType.equals("UUID")) {
					builderObject.append(row.getUUID(j));
				} else {
					DataSrcRestError err = DataSrcErrorList.buildError(new Exception("OOPS. Dev missed mapping for DATA Type.. Please raise a bug."), null, CmlpApplicationEnum.DATASOURCE);
					throw new DataSrcException("OOPS. Dev missed mapping. Please raise a bug.",
							Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
				}
			}

			builderObject.append("\r\n");

		} // end for loop
		log.info( "RETURN:CassandraDataSourceSvcImpl::buildSampleData");
		return builderObject;
	}// end method
	
	private int getRowCount(ResultSet results) {
		log.info( "ENTER:getRowCount");
		// countRow
		int rowCount = 0;
		Iterator<Row> itr = results.iterator();
		while (itr.hasNext()) {
			itr.next();
			rowCount++;
		}
		return rowCount;
	}
	
	public long getCassandraDatasetSize(ResultSet results, int rowCount) throws SQLException, IOException, DataSrcException {
		log.info( "ENTER:getCassandraDatasetSize");
		int MIN_SAMPLING_SIZE; //default to 50
		String strMIN_SAMPLING_SIZE = helperTool.getEnv("minimum_sampling_size",
				helperTool.getComponentPropertyValue("minimum_sampling_size"));
		MIN_SAMPLING_SIZE = strMIN_SAMPLING_SIZE != null ? Integer.parseInt(strMIN_SAMPLING_SIZE) : 50;

		long size = 0L;
		if (rowCount <= MIN_SAMPLING_SIZE) { 
			size = calculateCassandraDatasetSizeByNotSampling(results, rowCount);
		} else {
			size = calculateCassandraDatasetSizeBySampling(results, rowCount);
		}
		return size;
	}
	
	private long calculateCassandraDatasetSizeBySampling(ResultSet results, int rowCount) throws SQLException, DataSrcException {		
		log.info( "ENTER:calculateCassandraDatasetSizeBySampling");

		int columnCount = results.getColumnDefinitions().size();

		int sampleSize = (int) Math.ceil(rowCount / 5.0);
		int batchSize = (int) Math.ceil(sampleSize / 3.0);
		int middle = (int) Math.ceil(rowCount / 2.0);

		log.info("Number of Records: " + rowCount);
		log.info("SamplingSize: " + sampleSize);
		log.info("BatchSize: " + batchSize);
		log.info("Middle rowNumber: " + middle);

		StringBuilder builderObject = new StringBuilder();
		int rowNumber = 1;
		
		for (int i = 0; !results.isExhausted(); i++) {
			Row row = results.one();
			if ((rowNumber >= 1 && rowNumber < 1 + batchSize) || (rowNumber >= middle && rowNumber < middle + batchSize)
					|| (rowNumber >= (rowCount - batchSize + 1) && rowNumber <= rowCount)) {
				for (int j = 0; j < columnCount; j++) {
					if (j > 0) {
						builderObject.append(",");
					}
					// getting Java type for cassandra type
					String columnType = row.getColumnDefinitions().getType(j).getName().name();
			
					if (columnType.equals("ASCII") || columnType.equals("INET")
							|| columnType.equals("TEXT") || columnType.equals("TIMEUUID")
							|| columnType.equals("VARCHAR") || columnType.equals("TUPLE")) {
						builderObject.append(row.getString(j));
					} else if (columnType.equals("BIGINT") || columnType.equals("BOOLEAN")
							|| columnType.equals("COUNTER")) {
						builderObject.append(row.getLong(j));
					} else if (columnType.equals("DATE")) {
						builderObject.append(row.getDate(j));
					} else if (columnType.equals("DECIMAL")) {
						builderObject.append(row.getDecimal(j));
					} else if (columnType.equals("DOUBLE") || columnType.equals("FLOAT")) {
						builderObject.append(row.getDouble(j));
					} else if (columnType.equals("INT")) {
						builderObject.append(row.getInt(j));
					} else if (columnType.equals("SMALLINT")) {
						builderObject.append(row.getShort(j));
					} else if (columnType.equals("TINYINT")) {
						builderObject.append(row.getByte(j));
					} else if (columnType.equals("TIME")) {
						builderObject.append(row.getTime(j));
					} else if (columnType.equals("TIMESTAMP")) {
						builderObject.append(row.getTimestamp(j));
					} else if (columnType.equals("UUID")) {
						builderObject.append(row.getUUID(j));
					} else if (columnType.equals("BLOB")) {
						return -1L;
						// Cassandra has only BLOB columnType - does not have CLOB, NCLOB
					} else {
						DataSrcRestError err = DataSrcErrorList.buildError(new Exception("OOPS. Dev missed mapping for DATA Type.. Please raise a bug."), null, CmlpApplicationEnum.DATASOURCE);
						throw new DataSrcException("OOPS. Dev missed mapping. Please raise a bug.",
								Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
					}
				}
			}

			builderObject.append("\r\n");
			rowNumber++;
		}
		log.info("Past Last Record: " + rowNumber);
		long size = 0L;
		size = builderObject.toString().getBytes().length * 1L;
		return size * 5L;
	}
	
	private long calculateCassandraDatasetSizeByNotSampling(ResultSet results, int rowCount)
			throws SQLException, DataSrcException {

		log.info("calculateCassandraDatasetSizeByNotSampling");

		int columnCount = results.getColumnDefinitions().size();
		long size = 0L;

		log.info("Number of Records: " + rowCount);
		StringBuilder builderObject = new StringBuilder();
		int rowNumber = 1;

		for (int i = 0; !results.isExhausted(); i++) {
			Row row = results.one();

			for (int j = 0; j < columnCount; j++) {
				if (j > 0) {
					builderObject.append(",");
				}
				// getting Java type for cassandra type
				String columnType = row.getColumnDefinitions().getType(j).getName().name();

				if (columnType.equals("ASCII") || columnType.equals("INET") || columnType.equals("TEXT")
						|| columnType.equals("TIMEUUID") || columnType.equals("VARCHAR")
						|| columnType.equals("TUPLE")) {
					builderObject.append(row.getString(j));
				}  else if (columnType.equals("BIGINT") || columnType.equals("BOOLEAN")
						|| columnType.equals("COUNTER")) {
					builderObject.append(row.getLong(j));
				} else if (columnType.equals("DATE")) {
					builderObject.append(row.getDate(j));
				} else if (columnType.equals("DECIMAL")) {
					builderObject.append(row.getDecimal(j));
				} else if (columnType.equals("DOUBLE") || columnType.equals("FLOAT")) {
					builderObject.append(row.getDouble(j));
				} else if (columnType.equals("INT")) {
					builderObject.append(row.getInt(j));
				} else if (columnType.equals("SMALLINT")) {
					builderObject.append(row.getShort(j));
				} else if (columnType.equals("TINYINT")) {
					builderObject.append(row.getByte(j));
				} else if (columnType.equals("TIME")) {
					builderObject.append(row.getTime(j));
				} else if (columnType.equals("TIMESTAMP")) {
					builderObject.append(row.getTimestamp(j));
				} else if (columnType.equals("UUID")) {
					builderObject.append(row.getUUID(j));
				} else if (columnType.equals("BLOB")) {
					// Cassandra has only BLOB columnType - does not have CLOB, NCLOB
					return -1l;
				} else {
					DataSrcRestError err = DataSrcErrorList.buildError(new Exception("OOPS. Dev missed mapping for DATA Type.. Please raise a bug."), null, CmlpApplicationEnum.DATASOURCE);
					throw new DataSrcException("OOPS. Dev missed mapping. Please raise a bug.",
							Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
				}
			} // inner for loop
			log.info("Printing rows" + row.toString());
			builderObject.append("\r\n");
			rowNumber++;
		} // outer for loop
		log.info("Past Last Record: " + rowNumber);
		size = builderObject.toString().getBytes().length * 1L;

		log.info("Size of 100% dataset: " + size + " bytes");
		return size * 1L;
	}

	@Override
	public InputStream getSampleResults(DataSourceModelGet dataSource) throws DataSrcException {
		log.info("\nCassandraDataSourceSvcImpl::getSampleResults, using credetials, initializing cassandra cluster");
		applicationUtilities.validateConnectionParameters(dataSource);
		
		session = getSession(dataSource.getCommonDetails().getServerName(), 
				dataSource.getCommonDetails().getPortNumber(),
				dataSource.getDbDetails().getDbServerUsername().toLowerCase(),
				dataSource.getDbDetails().getDbServerPassword().toLowerCase());
		
		ResultSet results = session.execute(dataSource.getDbDetails().getDbQuery());
		
		StringBuilder builderObject = buildSampleData(results, DEFAULT_SAMPLE_SIZE );
		
		log.info( "\nCassandraDataSourceSvcImpl::getDatasetSampleData OUTPUT:\r\n\n" + builderObject);
		
		log.info("CassandraDataSourceSvcImpl::getDatasetSampleData, using credetials, closing cassandra session");
		session.close();
		log.info( "RETURN:CassandraDataSourceSvcImpl::getSampleResults");
		return new ByteArrayInputStream(builderObject.toString().getBytes());
	}
	
	
	private void raiseInvalidInputParameters() throws DataSrcException {
		ArrayList<String> missedParameters = new ArrayList<String>();
			
		missedParameters.add("dbServerUsername");
		missedParameters.add("dbServerPassword");
		missedParameters.add("dbQuery");
		missedParameters.add("serverName");
		missedParameters.add("portNumber");

	
		String[] variables = new String[missedParameters.size()];
	
		for(int i=0; i<missedParameters.size(); i++) {
			variables[i] = missedParameters.get(i);
		}
	
		DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
	
		throw new DataSrcException(
			"Datasource Model has missing mandatory information. Please send all the required information.", Status.BAD_REQUEST.getStatusCode(), err);

	}

}
