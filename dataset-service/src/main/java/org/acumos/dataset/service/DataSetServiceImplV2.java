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

package org.acumos.dataset.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Response.Status;

import org.acumos.dataset.common.CmlpApplicationEnum;
import org.acumos.dataset.common.CmlpErrorList;
import org.acumos.dataset.common.CmlpRestError;
import org.acumos.dataset.common.ErrorListEnum;
import org.acumos.dataset.connection.DbUtilities_v2;
import org.acumos.dataset.enums.DataSetTypeEnum;
import org.acumos.dataset.exception.DataSetException;
import org.acumos.dataset.schema.DataSetSearchKeys;
import org.acumos.dataset.schema.DatasetAttributeMetaData;
import org.acumos.dataset.schema.DatasetModelGet;
import org.acumos.dataset.schema.DatasetModelPost;
import org.acumos.dataset.schema.DatasetModelPut;
import org.acumos.dataset.utils.ApplicationUtilities;
import org.apache.http.entity.mime.FormBodyPart;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


@Service
public class DataSetServiceImplV2 implements DataSetServiceV2 {

	private static final String WRITEBACK_PREDICTION_FETCHING_DATASET = "writebackPrediction, fetching dataset";

	private static final String WRITEBACK_PREDICTION_DATASET_KEY = "writebackPrediction, datasetKey: ";

	private static final String WRITEBACK_PREDICTION_USER = "writebackPrediction, user: ";

	private static final String UPDATE_DATA_SOURCE_KEY_DATASOURCE_RESPONSE = "updateDataSourceKey(), Datasource Response: ";

	private static final String UPDATE_DATA_SOURCE_KEY_RECEIVED_OK_RESPONSE_FROM_DATA_SOURCE_MGR_READING_THE_DETAILS = "updateDataSourceKey(), Received OK response from DataSourceMgr. Reading the details...";

	private static final String INVALID_DATA_NO_DATASOURCE_KEY_INFO_PLEASE_SEND_VALID_INFORMATION = "Invalid data. No datasourceKey info. Please send valid information.";

	private static final String UPDATE_DATA_SOURCE_KEY_DATSETKEY = "updateDataSourceKey, datsetkey: ";

	private static final String UPDATE_DATA_SOURCE_KEY_USER = "updateDataSourceKey, user: ";

	private static final String UPDATE_ATTRIBUTE_META_DATA_DATSETKEY = "updateAttributeMetaData, datsetkey: ";

	private static final String UPDATE_ATTRIBUTE_META_DATA_USER = "updateAttributeMetaData, user: ";

	private static final String THE_GIVEN_DATASET_HAS_NO_DATASOURCE_ASSOCIATED_WITH_IT = "The given dataset has no datasource associated with it.";

	private static final String FETCHING_DATA_BASED_ON_USER_AND_DATASET_KEY = "fetching data based on user and dataset key";

	private static final String GET_DATA_SOURCES_DATSETKEY = "getDataSources, datsetkey: ";

	private static final String GET_DATA_SOURCES_USER = "getDataSources, user: ";

	private static final String DELETE = "delete";

	private static final String INVALID_DATASET_KEY_PLEASE_SEND_VALID_INFORMATION = "Invalid datasetKey. Please send valid information.";

	private static final String DELETE_DATA_SET_DETAIL_DATSETKEY = "deleteDataSetDetail, datsetkey: ";

	private static final String DELETE_DATA_SET_DETAIL_USER = "deleteDataSetDetail, user: ";

	private static final String READING_DATASET_DETAILS_FROM_DB = "Reading dataset details from db...";

	private static final String UPDATE = "update";

	private static final String INVALID_DATA_NO_DATASET_KEY_INFO_PLEASE_SEND_VALID_INFORMATION = "Invalid data. No datasetKey info. Please send valid information.";

	private static final String UPDATE_DATA_SET_DETAIL_DATSETKEY = "updateDataSetDetail, datsetkey: ";

	private static final String UPDATE_DATA_SET_DETAIL_USER = "updateDataSetDetail, user: ";

	private static final String UPDATE_DATA_SET_DETAIL_DATASOURCE_RESPONSE = "updateDataSetDetail()(), Datasource Response: ";

	private static final String UPDATE_DATA_SET_DETAIL_RECEIVED_OK_RESPONSE_FROM_DATA_SOURCE_MGR_READING_THE_DETAILS = "updateDataSetDetail(), Received OK response from DataSourceMgr. Reading the details...";

	private static final String THE_INPUT_HAS_A_DATA_SOURCE_KEY_ASSOCIATING_DATASOURCE = "The input has a DataSourceKey. Associating Datasource :";

	private static final String GET = "get";

	private static final String GET_DATA_SET_DETAILS_REECEIVED_SEARCHKEYS = "getDataSetDetails, Reeceived searchkeys: ";

	private static final String GET_DATA_SET_DETAILS_SEARCHKEY = "getDataSetDetails, searchkey: ";

	private static final String GET_DATA_SET_DETAILS_DATSETKEY = "getDataSetDetails, datsetkey: ";

	private static final String GET_DATA_SET_DETAILS_USER = "getDataSetDetails, user: ";

	private static final String INVALID_SEARCH_PLEASE_SEND_A_VALID_SEARCH = "Invalid search. Please send a valid search.";

	private static final String SEARCH_KEYS = "searchKeys";

	private static final String TEXT_SEARCH = "textSearch";

	private static final String RETURNING_DATASET_KEY = "returning dataset key: ";

	private static final String CALL_TO_INSERT_DATASET_DETAILS_INTO_DATABASE = "call to insert dataset details into database";

	private static final String THE_DATASET_HAS_BEEN_POPULATED_BY_FOLLOWING_KEY = "the dataset has been populated by following key: ";

	private static final String VALIDATE_DATASET_DETAILS_FOR_REQUIRED_FIELDS = "validate dataset details for required fields... ";

	private static final String INSERTION_OF_DATASET_DETAILS_WILL_BE_PERFORMED_BY_USER = "insertion of dataset details will be performed by user: ";

	private static final String WRITE = "write";

	private static final String READ_WRITE_DESCIPTOR = "readWriteDesciptor";

	private static final String INSERT_DATA_SET_DATASOURCE_RESPONSE = "insertDataSet(), Datasource Response: ";

	private static final String EXCEPTION_OCCURRED_WHILE_CALLING_DATASOURCE_MANAGER = "Exception occurred while calling Datasource Manager.";

	private static final String INSERT_DATA_SET_RECEIVED_OK_RESPONSE_FROM_DATA_SOURCE_MGR_READING_THE_DETAILS = "insertDataSet(), Received OK response from DataSourceMgr. Reading the details...";

	private static final String INTIATING_HTTP_CALL_FOR_FETCHING_DETAILS_ABOUT_A_DATASOURCE = "intiating http call for fetching details about a datasource";

	private static Logger log = LoggerFactory.getLogger(DataSetServiceImplV2.class);
	
	private static final String METADATA = "metaData";
	private static final String METADATAINFO = "metaDataInfo";
	private static final String COLUMN_COUNT = "columnCount";
	private static final String DATASET_KEY = "datasetKey";
	private static final String DATASOURCE_KEY = "datasourceKey";
	private static final String OWNED_BY = "ownedBy";
	private static final String PERMISSIONS_EXC = "Please check dataset key provided and user permission for this operation";
	private static final String DATASRC_NULL_EXC = "Null response from Datasource Manager with status code = 200";
	
	@Autowired
	private DbUtilities_v2 dbUtilities;
	
	private void insertDataset2(DatasetModelGet dataSet, String authorization) throws DataSetException, JSONException, IOException {
		String datasourceKey = dataSet.getDatasourceKey();
		log.info(INTIATING_HTTP_CALL_FOR_FETCHING_DETAILS_ABOUT_A_DATASOURCE);
		InputStream response = ApplicationUtilities.getDataSource(authorization, datasourceKey);

		log.info(INSERT_DATA_SET_RECEIVED_OK_RESPONSE_FROM_DATA_SOURCE_MGR_READING_THE_DETAILS);
		String strResponse = ApplicationUtilities.getDatasourceResponseFromInputStream(response).trim();

		// check for OK with no response message
		if (strResponse.length() == 0) {
			Exception exception = new Exception(DATASRC_NULL_EXC);
			
			CmlpRestError err = CmlpErrorList.buildError(exception, null, CmlpApplicationEnum.DATASET);
			throw new DataSetException(EXCEPTION_OCCURRED_WHILE_CALLING_DATASOURCE_MANAGER,
					Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
		}

		log.info(INSERT_DATA_SET_DATASOURCE_RESPONSE + strResponse);
		JSONObject jsonObj;
		
		if (strResponse.startsWith("[")) { //Array
			JSONArray jsonArray = new JSONArray(strResponse);
			jsonObj = jsonArray.getJSONObject(0);
		} else {
			jsonObj = new JSONObject(strResponse);
		}
		
		if (jsonObj.has(METADATA)) {
			JSONObject jsonObj1 = (JSONObject) jsonObj.get(METADATA);
			if(jsonObj1.has(METADATAINFO)) {
				JSONObject jsonObj2 = (JSONObject) jsonObj1.get(METADATAINFO);
				if(jsonObj2.has(COLUMN_COUNT))
					dataSet.setNoOfAttributes(Integer.valueOf(jsonObj2.getString(COLUMN_COUNT)));
				else
					dataSet.setNoOfAttributes(0);
			}
		} else {
			dataSet.setNoOfAttributes(0);
		}
		
		//set datasetType as writeback if datasource readWriteDesciptor = write
		if (jsonObj.has(READ_WRITE_DESCIPTOR)) {
			if(WRITE.equalsIgnoreCase(jsonObj.getString(READ_WRITE_DESCIPTOR))) {
				dataSet.getDatasetMetaData().setDatasetType(DataSetTypeEnum.WRITEBACK.getDatasetType());
			} else {
				if(dataSet.getDatasetMetaData().getDatasetType() == null || dataSet.getDatasetMetaData().getDatasetType().isEmpty())
					dataSet.getDatasetMetaData().setDatasetType(DataSetTypeEnum.EVALUATE.getDatasetType());
			}
		}
	}

	@Override
	public String insertDataSet(String authorization, String user, DatasetModelPost dataSetPost) throws DataSetException, IOException, JSONException
	{
		log.info(INSERTION_OF_DATASET_DETAILS_WILL_BE_PERFORMED_BY_USER + user);
		
		DatasetModelGet dataSet = new DatasetModelGet(dataSetPost);

		log.info(VALIDATE_DATASET_DETAILS_FOR_REQUIRED_FIELDS);
		ApplicationUtilities.validateDatasetModel(dataSet, dbUtilities, "create");

		dataSet.setOwnedBy(user);
		dataSet.setDatasetKey(ApplicationUtilities.getName(user));
		log.info(THE_DATASET_HAS_BEEN_POPULATED_BY_FOLLOWING_KEY + dataSet.getDatasetKey());

		if (dataSet.getDatasourceKey() != null && !dataSet.getDatasourceKey().isEmpty()) { //Associate Datasource
			insertDataset2(dataSet, authorization);
		}

		log.info(CALL_TO_INSERT_DATASET_DETAILS_INTO_DATABASE);
		dbUtilities.insertDataSetDetails(dataSet);

		log.info(RETURNING_DATASET_KEY + dataSet.getDatasetKey());
		return dataSet.getDatasetKey();
	}

	

	@Override
	public List<String> getDataSetDetails(String user, String datasetKey, String searchKey,
			DataSetSearchKeys dataSearchKeys) throws DataSetException, IOException {

		if (datasetKey == null && searchKey == null && dataSearchKeys == null) {
			String[] variables = { DATASET_KEY, TEXT_SEARCH, SEARCH_KEYS };
			CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum.E_0003, variables, null, CmlpApplicationEnum.DATASET);
			
			throw new DataSetException(INVALID_SEARCH_PLEASE_SEND_A_VALID_SEARCH,
					Status.BAD_REQUEST.getStatusCode(), err);
		}

		ArrayList<String> results = null;

		log.info(GET_DATA_SET_DETAILS_USER + user);

		if (datasetKey != null)
			log.info(GET_DATA_SET_DETAILS_DATSETKEY + datasetKey);
		if (searchKey != null)
			log.info(GET_DATA_SET_DETAILS_SEARCHKEY + searchKey);
		if (dataSearchKeys != null)
			log.info(GET_DATA_SET_DETAILS_REECEIVED_SEARCHKEYS + dataSearchKeys.toString());

		try {
			if (dataSearchKeys != null) { // Advanced search
				results = dbUtilities.getAdvancedSearchDetails(user, datasetKey, dataSearchKeys);

			} else { // Regular Search including global text search
				results = dbUtilities.getDataSetDetails(user, datasetKey, searchKey, GET);
			}
		} catch (DataSetException cmlpExc) {
			throw cmlpExc;
		} catch (Exception exc) {
			throw exc;
		}
		return results;
	}
	
	private void updateDataSetDetail2(DatasetModelGet dataSet, String authorization) throws DataSetException, IOException, JSONException {
		String datasourceKey = dataSet.getDatasourceKey();

		if (datasourceKey != null && datasourceKey.trim().length() > 0) { // Associating datasource
			log.info(THE_INPUT_HAS_A_DATA_SOURCE_KEY_ASSOCIATING_DATASOURCE + datasourceKey);
			InputStream inStream = ApplicationUtilities.getDataSource(authorization, datasourceKey);


			
			log.info(UPDATE_DATA_SET_DETAIL_RECEIVED_OK_RESPONSE_FROM_DATA_SOURCE_MGR_READING_THE_DETAILS);
			String strResponse = ApplicationUtilities.getDatasourceResponseFromInputStream(inStream);

			// check for OK with no response message
			if (strResponse.trim().length() == 0) {
				
				Exception exception = new Exception(DATASRC_NULL_EXC);
				
				CmlpRestError err = CmlpErrorList.buildError(exception, null, CmlpApplicationEnum.DATASET);
				throw new DataSetException(EXCEPTION_OCCURRED_WHILE_CALLING_DATASOURCE_MANAGER,
						Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
			}

			log.info(UPDATE_DATA_SET_DETAIL_DATASOURCE_RESPONSE + strResponse);

			// Read details from the stringbuffer and fill the
			// contents
			JSONObject jsonObj;
			if (strResponse.startsWith("[")) { //Array
				JSONArray jsonArray = new JSONArray(strResponse);
				jsonObj = jsonArray.getJSONObject(0);
			} else {
				jsonObj = new JSONObject(strResponse);
			}
			
			if (jsonObj.has(METADATA)) {
				JSONObject jsonObj1 = (JSONObject) jsonObj.get(METADATA);
				if(jsonObj1.has(METADATAINFO)) {
					JSONObject jsonObj2 = (JSONObject) jsonObj1.get(METADATAINFO);
					if(jsonObj2.has(COLUMN_COUNT))
						dataSet.setNoOfAttributes(Integer.valueOf(jsonObj2.getString(COLUMN_COUNT)));
					else
						dataSet.setNoOfAttributes(-1);
				}
			} else {
				dataSet.setNoOfAttributes(-1); // overwrite with 0
			}
		}
	}

	
	@Override
	public boolean updateDataSetDetail(String authorization, String user, String datasetKey, DatasetModelPut dataSetPut) throws DataSetException, IOException, JSONException {
		log.info(UPDATE_DATA_SET_DETAIL_USER + user);
		log.info(UPDATE_DATA_SET_DETAIL_DATSETKEY + datasetKey);
		
		DatasetModelGet dataSet = new DatasetModelGet(dataSetPut);

		if (datasetKey == null || datasetKey.trim().length() == 0 || dataSet == null) {
			String[] variables = { DATASET_KEY};
			CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum.E_0003, variables, null, CmlpApplicationEnum.DATASET);
			
			throw new DataSetException(INVALID_DATA_NO_DATASET_KEY_INFO_PLEASE_SEND_VALID_INFORMATION,
					Status.BAD_REQUEST.getStatusCode(), err);
		} else {
			dataSet.setDatasetKey(datasetKey); // Make sure DataSetModel has
													// always populated with
													// datasetKey
		}
		
		log.info(VALIDATE_DATASET_DETAILS_FOR_REQUIRED_FIELDS);
		ApplicationUtilities.validateDatasetModel(dataSet, dbUtilities, UPDATE);
			
		String dbDatasourceKey = null;

		// Check for Authorization to update the ticket
		log.info(READING_DATASET_DETAILS_FROM_DB);
		List<String> dbDatasets = dbUtilities.getDataSetDetails(user, datasetKey, null, UPDATE);

		if (dbDatasets != null && dbDatasets.isEmpty()) {
			CmlpRestError err;
			if(dbUtilities.isDatasetExists(datasetKey)) {
				String[] variables = {OWNED_BY};
				err = CmlpErrorList.buildError(ErrorListEnum.E_1003, variables, null, CmlpApplicationEnum.DATASET);
				throw new DataSetException(
						PERMISSIONS_EXC, Status.UNAUTHORIZED.getStatusCode(), err);
				
			} else {
				String[] variables = {DATASET_KEY};
				err = CmlpErrorList.buildError(ErrorListEnum.E_0003, variables, null, CmlpApplicationEnum.DATASET);
				
				throw new DataSetException(
						PERMISSIONS_EXC, Status.NOT_FOUND.getStatusCode(), err);
				
			}
		} else if(dbDatasets != null) {
			dbDatasourceKey = ApplicationUtilities.getFieldValueFromJsonString(dbDatasets.get(0), DATASOURCE_KEY);
			log.info("updateDataSetDetail(), The Datasource key in DB :" + dbDatasourceKey);
		}

		if (dataSet.getDatasourceKey() != null) {
			updateDataSetDetail2(dataSet, authorization);
		} else { // ignore the following fields in the request related to datasource
			dataSet.setNoOfAttributes(0);
		}
		
		return dbUtilities.updateDataSet(user, dataSet);
	}
	
	
	@Override
	public boolean deleteDataSetDetail(String user, String datasetKey) throws DataSetException, IOException {
		log.info(DELETE_DATA_SET_DETAIL_USER + user);
		log.info(DELETE_DATA_SET_DETAIL_DATSETKEY + datasetKey);

		if (datasetKey == null || datasetKey.trim().length() == 0) {
			String[] variables = { DATASET_KEY};
			CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum.E_0003, variables, null, CmlpApplicationEnum.DATASET);
			
			throw new DataSetException(INVALID_DATASET_KEY_PLEASE_SEND_VALID_INFORMATION,
					Status.BAD_REQUEST.getStatusCode(), err);
		}

		// Check for Authorization to update the ticket
		ArrayList<String> dbDataset = dbUtilities.getDataSetDetails(user, datasetKey, null, DELETE);

		if (dbDataset.size() == 0) {
			CmlpRestError err;
			if(dbUtilities.isDatasetExists(datasetKey)) {
				String[] variables = {OWNED_BY};
				err = CmlpErrorList.buildError(ErrorListEnum.E_1003, variables, null, CmlpApplicationEnum.DATASET);
				throw new DataSetException(
						PERMISSIONS_EXC, Status.UNAUTHORIZED.getStatusCode(), err);
				
			} else {
				String[] variables = {DATASET_KEY};
				err = CmlpErrorList.buildError(ErrorListEnum.E_0003, variables, null, CmlpApplicationEnum.DATASET);
				
				throw new DataSetException(
						PERMISSIONS_EXC, Status.NOT_FOUND.getStatusCode(), err);
				
			}
		}
		
		return dbUtilities.deleteDataSet(user, datasetKey);
	}

	
	@Override
	public String getDataSources(String authorization, String user, String datasetKey) throws DataSetException, IOException, JSONException {
		log.info(GET_DATA_SOURCES_USER + user);
		log.info(GET_DATA_SOURCES_DATSETKEY + datasetKey);
		
		log.info(FETCHING_DATA_BASED_ON_USER_AND_DATASET_KEY);
		ArrayList<String> datasetDetails = dbUtilities.getDataSetDetails(user, datasetKey, null, GET);

		if (datasetDetails.isEmpty()) {
			CmlpRestError err;
			
			if(dbUtilities.isDatasetExists(datasetKey)) {
				String[] variables = {OWNED_BY};
				err = CmlpErrorList.buildError(ErrorListEnum.E_1003, variables, null, CmlpApplicationEnum.DATASET);
				throw new DataSetException(
						PERMISSIONS_EXC, Status.UNAUTHORIZED.getStatusCode(), err);
				
			} else {
				String[] variables = {DATASET_KEY};
				err = CmlpErrorList.buildError(ErrorListEnum.E_0003, variables, null, CmlpApplicationEnum.DATASET);
				
				throw new DataSetException(
						PERMISSIONS_EXC, Status.BAD_REQUEST.getStatusCode(), err);
				
			}
		}

		String datasourceKey = ApplicationUtilities.getFieldValueFromJsonString(datasetDetails.get(0), DATASOURCE_KEY);
		
		if (datasourceKey == null) {
			String[] variables = {DATASOURCE_KEY};
			CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum.E_0003, variables, null, CmlpApplicationEnum.DATASET);
			
			throw new DataSetException(THE_GIVEN_DATASET_HAS_NO_DATASOURCE_ASSOCIATED_WITH_IT,
					Status.NOT_FOUND.getStatusCode(), err);
		}
		
		return datasourceKey;
	}

	
	@Override
	public boolean updateAttributeMetaData(String authorization, String user, String datasetKey, DatasetAttributeMetaData attributeMetaData)
			throws DataSetException, IOException {
		log.info(UPDATE_ATTRIBUTE_META_DATA_USER + user);
		log.info(UPDATE_ATTRIBUTE_META_DATA_DATSETKEY + datasetKey);
		
		if(!dbUtilities.isDatasetExists(datasetKey)) {
			String[] variables = {DATASET_KEY};
			CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum.E_0003, variables, null, CmlpApplicationEnum.DATASET);
			
			throw new DataSetException(INVALID_DATA_NO_DATASET_KEY_INFO_PLEASE_SEND_VALID_INFORMATION,
					Status.BAD_REQUEST.getStatusCode(), err);
		}
		
		return dbUtilities.updateAttributeMetaData(user, datasetKey, attributeMetaData);
	}
	
	
	@Override
	public boolean updateDataSourceKey(String authorization, String user, String datasetKey, String datasourceKey)
			throws DataSetException, IOException {
		log.info(UPDATE_DATA_SOURCE_KEY_USER + user);
		log.info(UPDATE_DATA_SOURCE_KEY_DATSETKEY + datasetKey);
		
		if(datasourceKey == null || datasourceKey.isEmpty()) {
			String[] variables = {DATASOURCE_KEY};
			CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum.E_0003, variables, null, CmlpApplicationEnum.DATASET);
			
			throw new DataSetException(INVALID_DATA_NO_DATASOURCE_KEY_INFO_PLEASE_SEND_VALID_INFORMATION,
					Status.BAD_REQUEST.getStatusCode(), err);
		}
		
		if(!dbUtilities.isDatasetExists(datasetKey)) {
			String[] variables = {DATASET_KEY};
			CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum.E_0003, variables, null, CmlpApplicationEnum.DATASET);
			
			throw new DataSetException(INVALID_DATA_NO_DATASET_KEY_INFO_PLEASE_SEND_VALID_INFORMATION,
					Status.BAD_REQUEST.getStatusCode(), err);
		}
		
		//Associate Datasource
		
		log.info(INTIATING_HTTP_CALL_FOR_FETCHING_DETAILS_ABOUT_A_DATASOURCE);
		InputStream response = ApplicationUtilities.getDataSource(authorization, datasourceKey);

		log.info(UPDATE_DATA_SOURCE_KEY_RECEIVED_OK_RESPONSE_FROM_DATA_SOURCE_MGR_READING_THE_DETAILS);
		String strResponse = ApplicationUtilities.getDatasourceResponseFromInputStream(response);

		// check for OK with no response message
		if (strResponse.trim().length() == 0) {
			Exception exception = new Exception(DATASRC_NULL_EXC);
			
			CmlpRestError err = CmlpErrorList.buildError(exception, null, CmlpApplicationEnum.DATASET);
			throw new DataSetException(EXCEPTION_OCCURRED_WHILE_CALLING_DATASOURCE_MANAGER,
					Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
		}

		log.info(UPDATE_DATA_SOURCE_KEY_DATASOURCE_RESPONSE + strResponse);

		return dbUtilities.updateDataSourceKey(user, datasetKey, datasourceKey, null);
	}

	@Override
	public void writebackPrediction(String user, String authorization, String datasetKey, FormBodyPart dataFile) 
		throws DataSetException, IOException, JSONException {
		log.info(WRITEBACK_PREDICTION_USER + user);
		log.info(WRITEBACK_PREDICTION_DATASET_KEY + datasetKey);
		
		log.info(WRITEBACK_PREDICTION_FETCHING_DATASET);
		ArrayList<String> datasetDetails = dbUtilities.getDataSetDetails(user, datasetKey, null, GET);

		if (datasetDetails.isEmpty()) {
			String[] variables = {DATASET_KEY};
			CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum.E_0003, variables, null, CmlpApplicationEnum.DATASET);			
			throw new DataSetException(INVALID_DATA_NO_DATASET_KEY_INFO_PLEASE_SEND_VALID_INFORMATION,
					Status.BAD_REQUEST.getStatusCode(), err);
		}
		
		String datasourceKey = ApplicationUtilities.getFieldValueFromJsonString(datasetDetails.get(0), DATASOURCE_KEY);	
		if (datasourceKey == null) {
			String[] variables = {DATASOURCE_KEY};
			CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum.E_0003, variables, null, CmlpApplicationEnum.DATASET);		
			throw new DataSetException(THE_GIVEN_DATASET_HAS_NO_DATASOURCE_ASSOCIATED_WITH_IT,
						Status.NOT_FOUND.getStatusCode(), err);
		}
		
		ApplicationUtilities.executeWriteback(authorization, datasourceKey, dataFile);
		
	}

}
