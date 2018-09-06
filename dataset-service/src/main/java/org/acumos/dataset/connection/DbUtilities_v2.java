/*-
 * ===============LICENSE_START=======================================================
 * Acumos
 * ===================================================================================
 * Copyright (C) 2017 AT&T Intellectual Property. All rights reserved.
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

package org.acumos.dataset.connection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.core.Response.Status;

import org.acumos.config.WebConfig;
import org.acumos.dataset.common.CmlpApplicationEnum;
import org.acumos.dataset.common.CmlpErrorList;
import org.acumos.dataset.common.CmlpRestError;
import org.acumos.dataset.common.DataSetSearchBuilder;
import org.acumos.dataset.common.ErrorListEnum;
import org.acumos.dataset.common.Utilities;
import org.acumos.dataset.exception.CmlpDataSrcException;
import org.acumos.dataset.schema.DataSetSearchKeys;
import org.acumos.dataset.schema.DatasetAttributeMetaData;
import org.acumos.dataset.schema.DatasetModelGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;

@Service
public class DbUtilities_v2 {

	private static final String GET_MONGO_COLLECTION_GET_MONGODB_CONNECTION_BEAN_FROM_WEB_CONFIG = "getMongoCollection(), get mongodb connection bean from WebConfig.";

	private static final String ID_OF_THE_INSERTED_MONGO_OBJECT = "id of the inserted mongo object: ";

	private static final String GET_DATA_SET_DETAILS_RETURNING_RESULTS = "getDataSetDetails(), Returning Results :";

	private static final String THE_DELETE_OPERATION_FOR_DATASET_KEY = "the delete operation for dataset key ";

	private static final String UPDATE_DATA_SET_ISSUING_COMMAND_TO_UPDATE_OBJECT_WITH_ID = "updateDataSet(), issuing command to update object with id: ";

	private static final String PLEASE_CHECK_THE_INPUT_DATA_OR_DATABASE_DETAILS = ". Please check the input data or database details.";

	private static final String UPDATE_DATA_SET_UPDATION_FAILED_FOR_OBJECT_WITH_ID = "updateDataSet(), updation failed for object with id: ";

	private static final String PLEASE_CHECK_DATASET_KEY_PROVIDED_AND_USER_PERMISSION_FOR_THIS_OPERATION = "please check dataset key provided and user permission for this operation";

	private static final String IS_DUPLICATE_RECORD_EXISTS_NO_DUPLICATE_RECORD_EXISTS = "isDuplicateRecordExists(), No duplicate record exists...";

	private static final String IS_DUPLICATE_RECORD_EXISTS_DUPLICATE_RECORD_EXISTS = "isDuplicateRecordExists(), Duplicate record exists...";

	private static final String IS_DUPLICATE_RECORD_EXISTS_PROCESSING_RESULTSET = "isDuplicateRecordExists(), processing resultset";

	private static final String IS_DUPLICATE_RECORD_EXISTS_RUNNING_QUERY = "isDuplicateRecordExists(), running query";

	private static final String NAMESPACE2 = "namespace";

	private static final String DATASET_NAME = "datasetName";

	private static final String IS_DUPLICATE_RECORD_EXISTS_PERFORMING_BASED_ON_DATASET_NAME_NAMEPACE_AND_ENVIRONMENT = "isDuplicateRecordExists(), performing based on DatasetName, Namepace and Environment";

	private static final String IS_DATASET_EXISTS_PROCESSING_RESULTSET = "isDatasetExists(), processing resultset";

	private static final String IS_DATASET_EXISTS_RUNNING_QUERY = "isDatasetExists(), running query";

	private static final String DATASET_KEY = "datasetKey";

	private static final String IS_DATASET_EXISTS_PERFORMING_BASED_ON_DATASET_KEY = "isDatasetExists(), performing based on datasetKey";

	private static final String GET_DATA_SET_DETAILS_RUNNING_ADVANCED_QUERY = "getDataSetDetails(), running advanced query : ";

	private static final String CHECK_FOR_DATASET_DATASOURCE_PROCESSING_RESULTSET = "checkForDatasetDatasource(), processing resultset";

	private static final String CHECK_FOR_DATASET_DATASOURCE_RUNNING_QUERY = "checkForDatasetDatasource(), running query : ";

	private static final String THE_UPDATE_ATTRIBUTE_META_DATA_FOR_DATASET_KEY = "the updateAttributeMetaData for dataset key ";

	private static final String UPDATE_ATTRIBUTE_META_DATA_RUNNING_QUERY = "updateAttributeMetaData(), running query : ";

	private static final String UPDATE_ATTRIBUTE_META_DATA_ISSUING_COMMAND_TO_UPDATE_OBJECT_WITH_ID = "updateAttributeMetaData(), issuing command to update object with id: ";

	private static final String SET = "$set";

	private static final String UPDATE_DATA_SOURCE_KEY_RUNNING_QUERY = "updateDataSourceKey(), running query : ";

	private static final String UPDATE_DATA_SOURCE_KEY_ISSUING_COMMAND_TO_UPDATE_OBJECT_WITH_ID = "updateDataSourceKey(), issuing command to update object with id: ";

	private static final String IS_ACTIVE = "isActive";

	private static final String OWNED_BY = "ownedBy";

	private static final String ID = "_id";

	private static final String INTIATING_QUERY_OBJECT = "intiating query object";

	private static final String THE_UPDATE_DATA_SOURCE_KEY_FOR_DATASET_KEY = "the updateDataSourceKey for dataset key ";

	private static final String WILL_BE_PERFORMED_BY_USER = " will be performed by user ";

	private static final String THE_UPDATE_OPERATION_FOR_DATASET_KEY = "the update operation for dataset key ";

	private static final String GET_DATA_SET_DETAILS_PROCESSING_RESULTSET = "getDataSetDetails(), processing resultset";

	private static final String GET_DATA_SET_DETAILS_RUNNING_QUERY = "getDataSetDetails(), running query : ";

	private static Logger log = LoggerFactory.getLogger(DbUtilities_v2.class);

	//@Autowired
	DBCollection datasetCollection;
	
	//@Autowired
	DB datasetDB;
	
	@Autowired
	private Environment env;

	
	private DBCollection getMongoCollection() throws IOException {
		log.info(GET_MONGO_COLLECTION_GET_MONGODB_CONNECTION_BEAN_FROM_WEB_CONFIG);
		if(datasetCollection == null) {
			String userName = env.getProperty("datasrc_mongo_username");
			String password = env.getProperty("datasrc_mongo_password");
			String dbName = env.getProperty("datasrc_mongo_dbname");
			String hostName = env.getProperty("datasrc_mongo_hostname");
			String portNumber = env.getProperty("datasrc_mongo_port");
			String collectionName = env.getProperty("datasrc_mongo_dbname");
			WebConfig config = new WebConfig();
			datasetCollection = config.getDBCollection(userName, password, dbName, hostName, portNumber, collectionName);
		}
		
		return datasetCollection;
	}
	
		
	public void setMongoCollection(DBCollection datasetCollection) {
		this.datasetCollection = datasetCollection;	
	}

	
	public void insertDataSetDetails(DatasetModelGet dataSet) throws IOException {
		DBObject dbObj = DataSetSearchBuilder.createDBObject(dataSet);
		WriteResult result = getMongoCollection().insert(dbObj);
		log.info(ID_OF_THE_INSERTED_MONGO_OBJECT + result.getUpsertedId());
	}
	
	
	public ArrayList<String> getDataSetDetails(String user, String datasetKey, String searchKey, String mode)
			throws CmlpDataSrcException, IOException {
		
		ArrayList<String> results = new ArrayList<String>();
		
		BasicDBObjectBuilder query = DataSetSearchBuilder.buildQueryForGet(user, datasetKey, mode);
		DBObject dbObj = query.get();
				
		log.info(GET_DATA_SET_DETAILS_RUNNING_QUERY + dbObj.toString());
		
		DBCursor cursor = getMongoCollection().find(dbObj);
		
		log.info(GET_DATA_SET_DETAILS_PROCESSING_RESULTSET);
		if(searchKey != null) {
		
			Pattern pattern = Pattern.compile(searchKey, Pattern.CASE_INSENSITIVE);
			Matcher m;
			String str_doc;
		
			DBObject temp;
		
			while (cursor.hasNext()) {
				temp = cursor.next();
				
				if(temp.containsField(ID))
					temp.removeField(ID);
				
				str_doc = temp.toString();
				m = pattern.matcher(str_doc);
				
				if(m.find()) {
					results.add(str_doc);
				}
			}
		} else {
			DBObject temp;
			
			while (cursor.hasNext()) {
				temp = cursor.next();
				
				if(temp.containsField(ID))
					temp.removeField(ID);
				
				results.add(temp.toString());
			}
		}
		
		log.info(GET_DATA_SET_DETAILS_RETURNING_RESULTS + results.size());
		
		return results;
	}


	
	public boolean updateDataSet(String user, DatasetModelGet dataset) throws CmlpDataSrcException, IOException {
		boolean updateDataset = false;
		WriteResult result = null;
		
		log.info(THE_UPDATE_OPERATION_FOR_DATASET_KEY + dataset.getDatasetKey() + WILL_BE_PERFORMED_BY_USER + user);
		log.info(INTIATING_QUERY_OBJECT);
		BasicDBObjectBuilder query = BasicDBObjectBuilder.start();
		
		query.add(ID, dataset.getDatasetKey());
		
		log.info(UPDATE_DATA_SET_ISSUING_COMMAND_TO_UPDATE_OBJECT_WITH_ID + dataset.getDatasetKey());
		
		DBObject dbObj = DataSetSearchBuilder.updateDBObject(dataset);
		
		log.info(GET_DATA_SET_DETAILS_RUNNING_QUERY + dbObj.toString());
		
		result = getMongoCollection().update(query.get(),
				new BasicDBObject().append(SET, dbObj));
		
		updateDataset = result.isUpdateOfExisting();
		
		if(!updateDataset) { //failed to update
			log.info(UPDATE_DATA_SET_UPDATION_FAILED_FOR_OBJECT_WITH_ID + dataset.getDatasetKey() + PLEASE_CHECK_THE_INPUT_DATA_OR_DATABASE_DETAILS);
			
			CmlpRestError err;
			if(isDatasetExists(dataset.getDatasetKey())) {
				String[] variables = {OWNED_BY};
				err = CmlpErrorList.buildError(ErrorListEnum.E_0003, variables, null, CmlpApplicationEnum.DATASET);
				throw new CmlpDataSrcException(
						PLEASE_CHECK_DATASET_KEY_PROVIDED_AND_USER_PERMISSION_FOR_THIS_OPERATION, Status.UNAUTHORIZED.getStatusCode(), err);
				
			} else {
				String[] variables = {DATASET_KEY};
				err = CmlpErrorList.buildError(ErrorListEnum.E_0003, variables, null, CmlpApplicationEnum.DATASET);
				
				throw new CmlpDataSrcException(
						PLEASE_CHECK_DATASET_KEY_PROVIDED_AND_USER_PERMISSION_FOR_THIS_OPERATION, Status.NOT_FOUND.getStatusCode(), err);
				
			}
		}
		
		return updateDataset;
	}
	
	
	
	public boolean deleteDataSet(String user, String datasetKey) throws IOException, CmlpDataSrcException {
		boolean deleteDataset = false;
		WriteResult result = null;
		
		log.info(THE_DELETE_OPERATION_FOR_DATASET_KEY + datasetKey + WILL_BE_PERFORMED_BY_USER + user);
		log.info(INTIATING_QUERY_OBJECT);
		BasicDBObjectBuilder query = BasicDBObjectBuilder.start();
		
		query.add(ID, datasetKey);
		
		log.info(UPDATE_DATA_SET_ISSUING_COMMAND_TO_UPDATE_OBJECT_WITH_ID + datasetKey);
		
		DBObject dbObj = DataSetSearchBuilder.deleteDBObject(datasetKey);
		
		log.info(GET_DATA_SET_DETAILS_RUNNING_QUERY + dbObj.toString());
		
		if(Utilities.isHardDeleteTurnedOn()) {
			result = getMongoCollection().remove(query.get());
			if(result.getN() > 0)
				deleteDataset = true;
		} else {
			result = getMongoCollection().update(query.get(),
						new BasicDBObject().append(SET, dbObj));
			deleteDataset = result.isUpdateOfExisting();
		}
		
		if(!deleteDataset) {
			log.info(UPDATE_DATA_SET_UPDATION_FAILED_FOR_OBJECT_WITH_ID + datasetKey + PLEASE_CHECK_THE_INPUT_DATA_OR_DATABASE_DETAILS);
			CmlpRestError err;
			if(isDatasetExists(datasetKey)) {
				String[] variables = {OWNED_BY};
				err = CmlpErrorList.buildError(ErrorListEnum.E_1003, variables, null, CmlpApplicationEnum.DATASET);
				throw new CmlpDataSrcException(
						PLEASE_CHECK_DATASET_KEY_PROVIDED_AND_USER_PERMISSION_FOR_THIS_OPERATION, Status.UNAUTHORIZED.getStatusCode(), err);
				
			} else {
				String[] variables = {DATASET_KEY};
				err = CmlpErrorList.buildError(ErrorListEnum.E_0003, variables, null, CmlpApplicationEnum.DATASET);
				
				throw new CmlpDataSrcException(
						PLEASE_CHECK_DATASET_KEY_PROVIDED_AND_USER_PERMISSION_FOR_THIS_OPERATION, Status.BAD_REQUEST.getStatusCode(), err);
				
			}
		}
		
		return deleteDataset;
	}
	
	
	
	public List<String> isDuplicateRecordExists(String datasetName, String namespace) throws IOException {
		BasicDBObjectBuilder query = BasicDBObjectBuilder.start();

		log.info(IS_DUPLICATE_RECORD_EXISTS_PERFORMING_BASED_ON_DATASET_NAME_NAMEPACE_AND_ENVIRONMENT);
		
		if (datasetName != null) {	
			query.add(DATASET_NAME, datasetName);
		}
	
		if (namespace != null) {
			query.add(NAMESPACE2, namespace);
		}
		
		//find _id only
		BasicDBObject fields = new BasicDBObject();
		fields.put(ID, 1);
		
		log.info(IS_DUPLICATE_RECORD_EXISTS_RUNNING_QUERY);
		log.info(IS_DUPLICATE_RECORD_EXISTS_PROCESSING_RESULTSET);
		List<String> datasetKeys = new ArrayList<>();
		
		if(!datasetKeys.isEmpty()) {
			log.info(IS_DUPLICATE_RECORD_EXISTS_DUPLICATE_RECORD_EXISTS);
		} else {
			log.info(IS_DUPLICATE_RECORD_EXISTS_NO_DUPLICATE_RECORD_EXISTS);
		}
		
		return datasetKeys;
	}
	
	public boolean isDatasetExists(String datasetKey) throws IOException {
		boolean isExists = false;
		
		if(datasetKey == null || datasetKey.isEmpty())
			return isExists;
		
		BasicDBObjectBuilder query = BasicDBObjectBuilder.start();

		log.info(IS_DATASET_EXISTS_PERFORMING_BASED_ON_DATASET_KEY);
		
		query.add(DATASET_KEY, datasetKey);
		
		//find _id only
		BasicDBObject fields = new BasicDBObject();
		fields.put(ID, 1);
		
		log.info(IS_DATASET_EXISTS_RUNNING_QUERY);
		DBObject dbResult = getMongoCollection().findOne(query.get(), fields);
		
		log.info(IS_DATASET_EXISTS_PROCESSING_RESULTSET);
		if(dbResult != null && dbResult.containsField(ID))
			isExists = true;
		
		return isExists;
	}
	
	
	public ArrayList<String> getAdvancedSearchDetails(String user, String datasetKey, DataSetSearchKeys dataSearchKeys)
			throws CmlpDataSrcException, IOException {
		
		ArrayList<String> results = new ArrayList<>();
		
		BasicDBObjectBuilder query = DataSetSearchBuilder.buildQueryForAdvancedSearch(user, datasetKey, dataSearchKeys);
		DBObject dbObj = query.get();
				
		log.info(GET_DATA_SET_DETAILS_RUNNING_ADVANCED_QUERY + dbObj.toString());
		
		DBCursor cursor = getMongoCollection().find(dbObj);
		
		log.info(GET_DATA_SET_DETAILS_PROCESSING_RESULTSET);
		
		while (cursor.hasNext()) {
			results.add(cursor.next().toString());
		}
		
		return results;
	}
	
	
	public boolean checkForDatasetDatasource(String user, String datasetKey, String datasourceKey) throws IOException, CmlpDataSrcException {
		boolean result = false;
		
		BasicDBObjectBuilder query = DataSetSearchBuilder.checkForDatasetDatasource(user, datasetKey, datasourceKey);
		DBObject dbObj = query.get();
				
		log.info(CHECK_FOR_DATASET_DATASOURCE_RUNNING_QUERY + dbObj.toString());
		
		DBCursor cursor = getMongoCollection().find(dbObj);
		
		log.info(CHECK_FOR_DATASET_DATASOURCE_PROCESSING_RESULTSET);

		if(cursor.hasNext()) {
			result = true;
		}
		
		return result;
	}
	
	
	public boolean updateAttributeMetaData(String user, String datasetKey, DatasetAttributeMetaData attributeMetaData) throws IOException {
		boolean isUpdated = false;
		WriteResult result = null;
		
		log.info(THE_UPDATE_ATTRIBUTE_META_DATA_FOR_DATASET_KEY + datasetKey + WILL_BE_PERFORMED_BY_USER + user);
		log.info(INTIATING_QUERY_OBJECT);
		BasicDBObjectBuilder query = BasicDBObjectBuilder.start();
		
		query.add(ID, datasetKey);
		query.append(OWNED_BY, user);
		query.append(IS_ACTIVE, true);
		
		log.info(UPDATE_ATTRIBUTE_META_DATA_ISSUING_COMMAND_TO_UPDATE_OBJECT_WITH_ID + datasetKey);
		
		DBObject dbObj = DataSetSearchBuilder.updateAttributeMetaData(null, attributeMetaData);
		
		log.info(UPDATE_ATTRIBUTE_META_DATA_RUNNING_QUERY + dbObj.toString());
		
		result = getMongoCollection().update(query.get(),
					new BasicDBObject().append(SET, dbObj));
		isUpdated = result.isUpdateOfExisting();
		
		
		return isUpdated;
	}
	
	
	public boolean updateDataSourceKey(String user, String datasetKey, String datasourceKey, DatasetAttributeMetaData attributeMetaData) throws IOException {
		boolean isUpdated = false;
		WriteResult result = null;
		
		log.info(THE_UPDATE_DATA_SOURCE_KEY_FOR_DATASET_KEY + datasetKey + WILL_BE_PERFORMED_BY_USER + user);
		log.info(INTIATING_QUERY_OBJECT);
		BasicDBObjectBuilder query = BasicDBObjectBuilder.start();
		
		query.add(ID, datasetKey);
		query.append(OWNED_BY, user);
		query.append(IS_ACTIVE, true);
		
		log.info(UPDATE_DATA_SOURCE_KEY_ISSUING_COMMAND_TO_UPDATE_OBJECT_WITH_ID + datasetKey);
		
		DBObject dbObj = DataSetSearchBuilder.updateAttributeMetaData(datasourceKey, attributeMetaData);
		
		log.info(UPDATE_DATA_SOURCE_KEY_RUNNING_QUERY + dbObj.toString());
		
		result = getMongoCollection().update(query.get(),
					new BasicDBObject().append(SET, dbObj));
		isUpdated = result.isUpdateOfExisting();
		
		
		return isUpdated;
	}
}
