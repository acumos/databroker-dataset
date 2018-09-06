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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response.Status;

import org.acumos.datasource.schema.DataSourceModelGet;
import org.acumos.datasource.schema.DataSourceMetadata;
import org.acumos.datasource.schema.NameValue;
import org.acumos.datasource.utils.ApplicationUtilities;

import com.mongodb.*;
import org.bson.BasicBSONEncoder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import org.slf4j.LoggerFactory;
import org.acumos.datasource.common.CmlpApplicationEnum;
import org.acumos.datasource.common.DataSrcErrorList;
import org.acumos.datasource.common.DataSrcRestError;
import org.acumos.datasource.common.ErrorListEnum;
import org.acumos.datasource.connection.DbUtilitiesV2;
import org.acumos.datasource.exception.DataSrcException;
import org.acumos.datasource.model.MongoDbConnectionModel;
import org.slf4j.Logger;

import com.mongodb.util.JSON;

@Service
public class MongoDataSourceSvcImpl implements MongoDataSourceSvc {

	private static Logger log = LoggerFactory.getLogger(MongoDataSourceSvcImpl.class);

	public MongoDataSourceSvcImpl() {
	}

	@Autowired
	private DbUtilitiesV2 dbUtilities;

	@Override
	public String getConnectionStatus_2_10(MongoDbConnectionModel objMongoDbConnectionModel, String query)
			throws UnknownHostException, IOException, DataSrcException {
		log.info("getConnectionStatus_2_10, trying to establish connection to mongo db on "
				+ objMongoDbConnectionModel.getHostname() + " using port " + objMongoDbConnectionModel.getPort()
				+ " for version greater than 2.10.");

		MongoClient mongo = null;
		DB db = null;
		String connectionMessage = "failed";

		if (objMongoDbConnectionModel.getHostname() != null && objMongoDbConnectionModel.getPort() != 0
				&& objMongoDbConnectionModel.getDbName() != null 
				) {

			log.info("getConnectionStatus_2_10, trying to establish mongo connection with credentials");
			log.info("getConnectionStatus_2_10, trying to establish mongo connection with credentials using username: "
					+ objMongoDbConnectionModel.getUsername());
			log.info("getConnectionStatus_2_10, trying to establish mongo connection with credentials for database: "
					+ objMongoDbConnectionModel.getDbName());

			mongo = getConnection(objMongoDbConnectionModel);

			db = mongo.getDB(objMongoDbConnectionModel.getDbName());
			if (query != null) {
				
				BasicDBObject dbQuery = (BasicDBObject)JSON.parse(query);
				
				DBCursor cursor = db.getCollection(objMongoDbConnectionModel.getCollectionName()).find(dbQuery);
				int numberOfDocs=0;
				double size =0;
                DBCollection coll = db.getCollection(objMongoDbConnectionModel.getCollectionName());
                while(cursor.hasNext()) {
                    numberOfDocs++;
                    connectionMessage = "success";
                    DBObject dbo= new BasicDBObject("_id",((BasicDBObject)cursor.next()).getString("_id") );
                    DBObject obj = coll.findOne(dbo);
                    if(obj != null) {
                    	int bsonSize = (new BasicBSONEncoder()).encode(obj).length;
                    	size +=bsonSize;
                    }
                }
                
                try {
                	if(connectionMessage.equals("success")) {
                		saveMetadata(objMongoDbConnectionModel,db,size,numberOfDocs, dbQuery);
                	}
                } catch (Exception e) {
                    log.error("Exception to save metadata : " + e.getMessage());
                    throw e;
                }
			} else if (db != null) {
				connectionMessage = "success";
			}

		}

		log.info("trying to establish connection to mongo db on " + objMongoDbConnectionModel.getHostname()
				+ " using port " + objMongoDbConnectionModel.getPort() + " for version greater than 2.10.");

		log.info("Connection has been established to mongo db on " + objMongoDbConnectionModel + " using port "
				+ objMongoDbConnectionModel + " for version greater than 2.10.");

		return connectionMessage;
	}

	@Override
	public MongoClient getConnection(MongoDbConnectionModel objMongoDbConnectionModel)
			throws UnknownHostException, DataSrcException, IOException {
		MongoClient mongo = null;

		if(objMongoDbConnectionModel.getPassword() != null && !objMongoDbConnectionModel.getPassword().isEmpty()) {
			log.info("getConnection, trying to establish mongo connection with credentials");
			log.info("getConnection, trying to establish mongo connection with credentials using username: "
					+ objMongoDbConnectionModel.getUsername());
			log.info("getConnection, trying to establish mongo connection with credentials for database: "
					+ objMongoDbConnectionModel.getDbName());
			
			MongoCredential credential = MongoCredential.createCredential(objMongoDbConnectionModel.getUsername(),
					objMongoDbConnectionModel.getDbName(), objMongoDbConnectionModel.getPassword().toCharArray());
			mongo = new MongoClient(
					new ServerAddress(objMongoDbConnectionModel.getHostname(), objMongoDbConnectionModel.getPort()),
					Arrays.asList(credential));
		} else {
			log.info("getConnection, trying to establish mongo connection with no credentials");
			log.info("getConnection, trying to establish mongo connection with no credentials for database: "
					+ objMongoDbConnectionModel.getDbName());
			
			mongo = new MongoClient(
					new ServerAddress(objMongoDbConnectionModel.getHostname(), objMongoDbConnectionModel.getPort()));
		}
		
		return mongo;
	}

	@Override
	public InputStream getResults(String user, String authorization, String namespace, String datasourceKey)
			throws DataSrcException, IOException {

		ArrayList<String> dbDatasourceDetails = dbUtilities.getDataSourceDetails(user, null, null, datasourceKey, null, true, false, authorization);
		
		DataSourceModelGet dbDataSource = ApplicationUtilities.getDataSourceModel(dbDatasourceDetails.get(0));
		
		if (dbDataSource.getCategory().equals("mongo") && dbDataSource.getOwnedBy().equals(user)) {
			MongoClient mongo = null;
			DB db = null;

			// initializing and populating a map that has key value pair for
			// credentials
			StringBuilder builderObject = new StringBuilder();
			Map<String, String> decryptionMap = new HashMap<>();

			decryptionMap = ApplicationUtilities.readFromMongoCodeCloud(user, datasourceKey);

			// initializing and populating MongoDbConnectionModel
			MongoDbConnectionModel objMongoDbConnectionModel = new MongoDbConnectionModel();
			objMongoDbConnectionModel.setDbName(dbDataSource.getDbDetails().getDatabaseName());
			objMongoDbConnectionModel.setHostname(dbDataSource.getCommonDetails().getServerName());
			objMongoDbConnectionModel.setPassword(decryptionMap.get("dbServerPassword".toLowerCase()));
			objMongoDbConnectionModel.setPort(dbDataSource.getCommonDetails().getPortNumber());
			objMongoDbConnectionModel.setUsername(decryptionMap.get("dbServerUsername".toLowerCase()));
			objMongoDbConnectionModel.setCollectionName(dbDataSource.getDbDetails().getDbCollectionName());

			// initializing a query
			String query = dbDataSource.getDbDetails().getDbQuery();

			log.info("getResults, using credetials, initializing mongo connection");
			mongo = getConnection(objMongoDbConnectionModel);

			if(mongo != null) {
				// getting db instance from mongo
				db = mongo.getDB(objMongoDbConnectionModel.getDbName());
	
				if(db != null) {
					
					BasicDBObject dbQuery = (BasicDBObject)JSON.parse(query);
		
					// fetching result
					DBCursor cursor = db.getCollection(objMongoDbConnectionModel.getCollectionName()).find(dbQuery);
		
					// parsing resultset
					while (cursor.hasNext()) {
						builderObject.append(cursor.next().toString());
					}
				}
			}
			return new ByteArrayInputStream(builderObject.toString().getBytes());
		}
		
		//No information
		String[] variables = {"datasourceKey"};
		
		DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
		
		throw new DataSrcException("No Results found for the given DatasourceKey.",
				Status.NOT_FOUND.getStatusCode(), err);

	}

	private void saveMetadata(MongoDbConnectionModel objMongoDbConnectionModel,
							 DB db,double size, int numberOfDocs,BasicDBObject query1) throws DataSrcException
    {
		DataSourceMetadata m = new DataSourceMetadata();
        ArrayList<NameValue> lstSimpleMetadata = new ArrayList<NameValue>();
        lstSimpleMetadata.add(new NameValue("noOfDocs", String.valueOf(numberOfDocs)));
        lstSimpleMetadata.add(new NameValue("sizeOfDocs", String.valueOf(size)+"bytes"));
        m.setMetaDataInfo(lstSimpleMetadata);


        List<DBObject>  indexList = db.getCollection(objMongoDbConnectionModel.
				getCollectionName()).getIndexInfo();

		for(DBObject dbobj: indexList)
		{
			((BasicDBObject)dbobj).remove("weights");
			((BasicDBObject)dbobj).remove("v");

		}
        m.setMetaDataInfo(lstSimpleMetadata);
//        m.setListIndex(indexList);
        objMongoDbConnectionModel.setMetadata(m);
	}


	@Override
	public InputStream getSampleResults (String user, String authorization, String namespace, String datasourceKey)
			throws DataSrcException, IOException {
		// checks not null condition fro user and datasourcekey
		log.info("getSampleResults, checking null condition for user and datasourcekey");

		ArrayList<String> dbDatasourceDetails = dbUtilities.
				getDataSourceDetails(user, null, null,
						datasourceKey, null, true, false, authorization);

		DataSourceModelGet dbDataSource = ApplicationUtilities.getDataSourceModel(dbDatasourceDetails.get(0));
		
		log.info("getSampleResults, category test and user checking for mongo datasource will be initiated");
		if (dbDataSource.getCategory().equals("mongo") &&
				dbDataSource.getOwnedBy().equals(user)) {
			MongoClient mongo = null;
			DB db = null;

			// initializing and populating a map that has key value pair for
			// credentials
			StringBuilder builderObject = new StringBuilder();
			Map<String, String> decryptionMap = new HashMap<>();

			decryptionMap = ApplicationUtilities.readFromMongoCodeCloud(user, datasourceKey);

			// initializing and populating MongoDbConnectionModel
			MongoDbConnectionModel objMongoDbConnectionModel = new MongoDbConnectionModel();
			objMongoDbConnectionModel.setDbName(dbDataSource.getDbDetails().getDatabaseName());
			objMongoDbConnectionModel.setHostname(dbDataSource.getCommonDetails().getServerName());
			objMongoDbConnectionModel.setPassword(decryptionMap.get("dbServerPassword".toLowerCase()));
			objMongoDbConnectionModel.setPort(dbDataSource.getCommonDetails().getPortNumber());
			objMongoDbConnectionModel.setUsername(decryptionMap.get("dbServerUsername".toLowerCase()));
			objMongoDbConnectionModel.setCollectionName(dbDataSource.getDbDetails().getDbCollectionName());

			// initializing a query
			log.info("getSampleResults, getting query from the objconnectionmodel payload provided");
			String query = dbDataSource.getDbDetails().getDbQuery();

			log.info("getSampleResults, using credetials, initializing mongo connection");
			mongo = getConnection(objMongoDbConnectionModel);

			if(mongo != null) {
				// getting db instance from mongo
				db = mongo.getDB(objMongoDbConnectionModel.getDbName());
	
				if(db != null) {
					
					BasicDBObject dbQuery = (BasicDBObject)JSON.parse(query);
		
					// fetching result
					log.info("getSampleResults, limiting the query result to 5 documents");
					DBCursor cursor = db.getCollection(objMongoDbConnectionModel.getCollectionName()).
							find(dbQuery).limit(5);
		
					// parsing resultset
					while (cursor.hasNext()) {
						builderObject.append(cursor.next().toString());
		
					}
				}
			}

			return new ByteArrayInputStream(builderObject.toString().getBytes());
		} 

		//No Information
		String[] variables = {"datasourceKey"};
		
		DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
		
		throw new DataSrcException("No Results found for the given DatasourceKey.",
				Status.NOT_FOUND.getStatusCode(), err);
	}

	@Override
	public InputStream getSampleResults(DataSourceModelGet dataSource) throws UnknownHostException, IOException, NumberFormatException, DataSrcException {
		log.info("getSampleResultsBeforeRegistration, checking null condition");
		ApplicationUtilities.validateConnectionParameters(dataSource);
		
		// initializing 
		StringBuilder builderObject = new StringBuilder();
		
		// initializing and populating MongoDbConnectionModel
		MongoDbConnectionModel objMongoDbConnectionModel = new MongoDbConnectionModel();
		objMongoDbConnectionModel.setDbName(dataSource.getDbDetails().getDatabaseName());
		objMongoDbConnectionModel.setUsername(dataSource.getDbDetails().getDbServerUsername());
		objMongoDbConnectionModel.setPassword(dataSource.getDbDetails().getDbServerPassword());
		objMongoDbConnectionModel.setHostname(dataSource.getCommonDetails().getServerName());
		objMongoDbConnectionModel.setPort(dataSource.getCommonDetails().getPortNumber());
		objMongoDbConnectionModel.setCollectionName(dataSource.getDbDetails().getDbCollectionName());

		// initializing a query
		log.info("getSampleResults, getting query from the objconnectionmodel payload provided");

		log.info("getSampleResults, using credetials, initializing mongo connection");
		MongoClient mongo = getConnection(objMongoDbConnectionModel);

		if(mongo != null) {
			// getting db instance from mongo
			DB db = mongo.getDB(objMongoDbConnectionModel.getDbName());
	
			if(db != null) {
				BasicDBObject dbQuery = (BasicDBObject)JSON.parse(dataSource.getDbDetails().getDbQuery());
		
				// fetching result
				log.info("getSampleResults, limiting the query result to 5 documents");
				DBCursor cursor = db.getCollection(objMongoDbConnectionModel.getCollectionName()).
						find(dbQuery).limit(5);
		
				// parsing resultset
				while (cursor.hasNext()) {
					builderObject.append(cursor.next().toString());
		
				}
			}
		}

		return new ByteArrayInputStream(builderObject.toString().getBytes());
	}

}
