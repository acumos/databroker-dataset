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

package org.acumos.service.common;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.acumos.service.exception.CmlpDataSrcException;
import org.acumos.service.schema.DataMetaDataInfo;
import org.acumos.service.schema.DataSetKVPair;
import org.acumos.service.schema.DataSetSearchKeys;
import org.acumos.service.schema.DatasetAttributeMetaData;
import org.acumos.service.schema.DatasetModelGet;
import org.acumos.service.schema.DatasetTransformationInfo;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

public class DataSetSearchBuilder {

	private static final String OWNEDBY = "ownedBy";
	private static final String NAMESPACE = "namespace";
	private static final String ATTRIBUTETYPE = "attributeType";
	private static final String DATASETDESCRIPTION = "datasetDescription";
	private static final String DATASETNAME = "datasetName";
	private static final String DATASETTYPE = "datasetType";
	private static final String DOMAINAREA = "domainArea";
	private static final String FORMATTYPE = "formatType";
	private static final String NOOFATTR = "noOfAttributes";
	private static final String TASKTYPE = "taskType";
	private static final String ISACTIVE = "isActive";
	private static final String DATASOURCEKEY = "datasourceKey";
	private static final String DATASETKEY = "datasetKey";
	private static final String COLUMNNAMES = "columnNames";
	private static final String COLUMNTYPES = "columnTypes";
	private static final String EXPRESSION = "expression";
	private static final String TRANSFORMATION = "transformations";
	private static final String ATTMETADATA = "attributeMetaData";
	private static final String COLUMNINDEX = "columnIndex";
	private static final String VALUE = "value";

	private DataSetSearchBuilder() {
		super();
	}

	public static BasicDBObjectBuilder buildQueryForAdvancedSearch(String user, String datasetKey, DataSetSearchKeys searchKeys) {

		BasicDBObjectBuilder query = BasicDBObjectBuilder.start();

		List<BasicDBObject> users = new ArrayList<>();
		users.add(new BasicDBObject(OWNEDBY, user));
		users.add(new BasicDBObject("sharedUsers.user", user)); // user in
																// shared users
		users.add(new BasicDBObject("shareDatasetToAll", true));
		query.add("$or", users);

		if (datasetKey != null && datasetKey.trim().length() > 0)
			query.add("_id", datasetKey);

		if (searchKeys != null) {
			setQueryObject(datasetKey, searchKeys, query);
			setQueryObjectForNoOfInstances(searchKeys, query);
			setQueryDataMetaDataObject(searchKeys, query);
			setQueryObjectForNoOfAttributes(searchKeys, query);
			setQueryObjectForDataSetKVPair(searchKeys, query);
			query.add(ISACTIVE, true);
		}

		return query;
	}

	private static void setQueryObjectForNoOfAttributes(DataSetSearchKeys searchKeys, BasicDBObjectBuilder query) {
		if (searchKeys.getNoOfAttributes() != null) {
			List<BasicDBObject> attributeRanges = new ArrayList<>();
			int[] range;

			for (String attributeRange : searchKeys.getNoOfAttributes()) { // 10-100
				range = Utilities.getRange(attributeRange);

				if (range[1] != 0) {
					attributeRanges.add(
							new BasicDBObject(NOOFATTR, new BasicDBObject("$gte", range[0]).append("$lte", range[1])));
				}
			}

			if (!attributeRanges.isEmpty())
				query.add("$or", attributeRanges);
		}
	}

	private static void setQueryObjectForDataSetKVPair(DataSetSearchKeys searchKeys, BasicDBObjectBuilder query) {
		if (searchKeys.getKeyValuePairs() != null) {
			List<String> kvKeyInfo = new ArrayList<>();
			List<String> kvValueInfo = new ArrayList<>();
			for (DataSetKVPair kvPair : searchKeys.getKeyValuePairs()) {
				if (kvPair.getKey() != null) {
					kvKeyInfo.add(kvPair.getKey());
				}
				if (kvPair.getValue() != null) {
					kvValueInfo.add(kvPair.getValue());
				}
			}

			if (!kvKeyInfo.isEmpty()) {
				query.add("keyValuePairs.key", new BasicDBObject("$in", kvKeyInfo));
			}

			if (!kvValueInfo.isEmpty()) {
				query.add("keyValuePairs.value", new BasicDBObject("$in", kvValueInfo));
			}
		}
	}

	private static void setQueryObjectForNoOfInstances(DataSetSearchKeys searchKeys, BasicDBObjectBuilder query) {
		if (searchKeys.getNoOfInstances() != null) {
			List<BasicDBObject> instanceRanges = new ArrayList<>();
			int[] range;

			for (String instanceRange : searchKeys.getNoOfInstances()) { // 10-100
				range = Utilities.getRange(instanceRange);

				if (range[1] != 0) {
					instanceRanges.add(new BasicDBObject("noOfInstances",
							new BasicDBObject("$gte", range[0]).append("$lte", range[1])));
				}
			}

			if (!instanceRanges.isEmpty())
				query.add("$or", instanceRanges);
		}
	}

	private static void setQueryDataMetaDataObject(DataSetSearchKeys searchKeys,
			BasicDBObjectBuilder query) {

		if (searchKeys.getDataMetaDataInfo() != null) {
			List<String> metaDetailInfo = new ArrayList<>();
			List<String> metaValueInfo = new ArrayList<>();
			for (DataMetaDataInfo meta : searchKeys.getDataMetaDataInfo()) {
				if (meta.getDetail() != null) {
					metaDetailInfo.add(meta.getDetail());
				}
				if (meta.getValue() != null) {
					metaValueInfo.add(meta.getValue());
				}
			}

			if (!metaDetailInfo.isEmpty()) {
				query.add("dataMetaDataInfo.detail", new BasicDBObject("$in", metaDetailInfo));
			}

			if (!metaValueInfo.isEmpty()) {
				query.add("dataMetaDataInfo.value", new BasicDBObject("$in", metaValueInfo));
			}
		}

	}

	private static void setQueryObject(String datasetKey, DataSetSearchKeys searchKeys, BasicDBObjectBuilder query) {
		if (searchKeys.getDatasetKey() != null && datasetKey == null) {
			query.add("_id", searchKeys.getDatasetKey());
		}

		if (searchKeys.getNamespace() != null)
			query.add(NAMESPACE, searchKeys.getNamespace());

		if (searchKeys.getAttributeType() != null)
			query.add(ATTRIBUTETYPE, searchKeys.getAttributeType());

		if (searchKeys.getDatasetDataType() != null)
			query.add("datasetDataType", searchKeys.getDatasetDataType());

		if (searchKeys.getDatasetDescription() != null)
			query.add(DATASETDESCRIPTION, searchKeys.getDatasetDescription());

		if (searchKeys.getDatasetName() != null)
			query.add(DATASETNAME, searchKeys.getDatasetName());

		if (searchKeys.getDatasetType() != null)
			query.add(DATASETTYPE, searchKeys.getDatasetType());

		if (searchKeys.getDatasourceName() != null)
			query.add("datasourceName", searchKeys.getDatasourceName());

		if (searchKeys.getDomainArea() != null)
			query.add(DOMAINAREA, searchKeys.getDomainArea());

		if (searchKeys.getFormatType() != null)
			query.add(FORMATTYPE, searchKeys.getFormatType());

		if (searchKeys.getTaskType() != null) {
			query.add(TASKTYPE, searchKeys.getTaskType());
		}
	}

	public static BasicDBObjectBuilder buildQueryForGet(String user, String datasetKey, String mode)
			throws CmlpDataSrcException {

		BasicDBObjectBuilder query = BasicDBObjectBuilder.start();

		if (datasetKey != null && datasetKey.trim().length() > 0) {
			if (!datasetKey.equalsIgnoreCase("ALL")) { // fetch the given datasetKey

				query.add("_id", datasetKey);
			}

			if ("update".equals(mode) || "delete".equals(mode)) {
				query.add(OWNEDBY, user);
				query.add(ISACTIVE, true);
				return query;
			}
		}
		query.add(OWNEDBY, user);
		query.add(ISACTIVE, true);

		return query;
	}

	public static DBObject createDBObject(DatasetModelGet dataSet) {
		BasicDBObjectBuilder dataSetBuilder = BasicDBObjectBuilder.start();

		buildDataSetBuilder(dataSet, dataSetBuilder);
		setDatasetMetadata(dataSet, dataSetBuilder);

		if (dataSet.getCustomMetaData() != null) {
			List<BasicDBObject> customMetaData = new ArrayList<>();

			for (DataSetKVPair detail : dataSet.getCustomMetaData()) {
				customMetaData.add((new BasicDBObject("key", detail.getKey())).append(VALUE, detail.getValue()));
			}
			dataSetBuilder.append("customMetaData", customMetaData);
		}

		setAttributeMetaData(dataSet.getAttributeMetaData(), dataSetBuilder);

		if (dataSet.getOwnedBy() != null) {
			dataSetBuilder.append(OWNEDBY, dataSet.getOwnedBy());
		}

		dataSetBuilder.append("isHeaderRowAvailable", dataSet.isHeaderRowAvailable());

		dataSetBuilder.append("createdTimeStamp",
				new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss z").format(Calendar.getInstance().getTime()));

		dataSetBuilder.append(ISACTIVE, true);

		dataSetBuilder.append("version", "v2");

		return dataSetBuilder.get();

	}

	public static void buildDataSetBuilder(DatasetModelGet dataSet, BasicDBObjectBuilder dataSetBuilder) {
		dataSetBuilder.append("_id", dataSet.getDatasetKey());
		dataSetBuilder.append(DATASETKEY, dataSet.getDatasetKey());

		if (dataSet.getNamespace() != null) {
			dataSetBuilder.append(NAMESPACE, dataSet.getNamespace());
		}

		if (dataSet.getDatasetName() != null) {
			dataSetBuilder.append(DATASETNAME, dataSet.getDatasetName());
		}

		if (dataSet.getDatasetDescription() != null) {
			dataSetBuilder.append(DATASETDESCRIPTION, dataSet.getDatasetDescription());
		}

		if (dataSet.getDatasourceKey() != null) {
			dataSetBuilder.append(DATASOURCEKEY, dataSet.getDatasourceKey());
		}

		if (dataSet.getNoOfAttributes() != 0) {
			dataSetBuilder.append(NOOFATTR, dataSet.getNoOfAttributes());
		} else {
			dataSetBuilder.append(NOOFATTR, 0);
		}

		if (dataSet.getServiceMetaData() != null) {
			List<BasicDBObject> serviceMetaData = new ArrayList<>();

			for (DataSetKVPair detail : dataSet.getServiceMetaData()) {
				serviceMetaData.add((new BasicDBObject("key", detail.getKey())).append(VALUE, detail.getValue()));
			}
			dataSetBuilder.append("serviceMetaData", serviceMetaData);
		}
	}

	public static void setAttributeMetaData(DatasetAttributeMetaData attributeMetaData,
			BasicDBObjectBuilder dataSetBuilder) {
		if (attributeMetaData != null) {

			BasicDBObject attributeMetadata = new BasicDBObject();

			if (attributeMetaData.getColumnNames() != null) {
				attributeMetadata.append(COLUMNNAMES, attributeMetaData.getColumnNames());
			}

			if (attributeMetaData.getColumnTypes() != null) {
				attributeMetadata.append(COLUMNTYPES, attributeMetaData.getColumnTypes());
			}

			if (attributeMetaData.getTransformations() != null) {
				setAttributeTransformationMetaData(attributeMetaData, attributeMetadata);
			}

			dataSetBuilder.append(ATTMETADATA, attributeMetadata);
		}
	}

	public static void setAttributeTransformationMetaData(DatasetAttributeMetaData attributeMetaData,
			BasicDBObject attributeMetadata) {

		List<BasicDBObject> details = new ArrayList<>();

		for (DatasetTransformationInfo detail : attributeMetaData.getTransformations()) {
			// TO DO: check alll parameters and populate it
			BasicDBObject tableInfo = new BasicDBObject();
			if (detail.getColumnIndex() != 0) {
				tableInfo.append(COLUMNINDEX, detail.getColumnIndex());
			} else {
				tableInfo.append(COLUMNINDEX, 0);
			}

			if (detail.getType() != null) {
				tableInfo.append("type", detail.getType());
			}

			if (detail.getExpression() != null) {
				tableInfo.append(EXPRESSION, detail.getExpression());
			}
			details.add(tableInfo);
		}
		attributeMetadata.append(TRANSFORMATION, details);
	}

	public static void setDatasetMetadata(DatasetModelGet dataSet, BasicDBObjectBuilder dataSetBuilder) {
		if (dataSet.getDatasetMetaData() != null) {
			BasicDBObject datasetMetadata = new BasicDBObject();
			if (dataSet.getDatasetMetaData().getAttributeType() != null)
				datasetMetadata.append(ATTRIBUTETYPE, dataSet.getDatasetMetaData().getAttributeType());

			if (dataSet.getDatasetMetaData().getGeoStatisticalType() != null)
				datasetMetadata.append("geoStatisticalType", dataSet.getDatasetMetaData().getGeoStatisticalType());

			if (dataSet.getDatasetMetaData().getDatasetType() != null)
				datasetMetadata.append(DATASETTYPE, dataSet.getDatasetMetaData().getDatasetType());

			if (dataSet.getDatasetMetaData().getDomainArea() != null)
				datasetMetadata.append(DOMAINAREA, dataSet.getDatasetMetaData().getDomainArea());

			if (dataSet.getDatasetMetaData().getFormatType() != null)
				datasetMetadata.append(FORMATTYPE, dataSet.getDatasetMetaData().getFormatType());

			if (dataSet.getDatasetMetaData().getTaskType() != null)
				datasetMetadata.append(TASKTYPE, dataSet.getDatasetMetaData().getTaskType());

			dataSetBuilder.append("datasetMetaData", datasetMetadata);
		}
	}

	public static DBObject updateDBObject(DatasetModelGet dataSet) {
		BasicDBObjectBuilder dataSetBuilder = BasicDBObjectBuilder.start();

		buildDataSetBuilder(dataSet, dataSetBuilder);
		setDatasetMetadata(dataSet, dataSetBuilder);

		if (dataSet.getCustomMetaData() != null) {
			List<BasicDBObject> customMetaData = new ArrayList<>();

			for (DataSetKVPair detail : dataSet.getCustomMetaData()) {
				customMetaData.add((new BasicDBObject("key", detail.getKey())).append(VALUE, detail.getValue()));
			}
			dataSetBuilder.append("customMetaData", customMetaData);
		}

		setAttributeMetaData(dataSet.getAttributeMetaData(), dataSetBuilder);

		if (dataSet.getOwnedBy() != null) {
			dataSetBuilder.append(OWNEDBY, dataSet.getOwnedBy());
		}

		dataSetBuilder.append("isHeaderRowAvailable", dataSet.isHeaderRowAvailable());

		dataSetBuilder.append("updatedTimeStamp",
				new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss z").format(Calendar.getInstance().getTime()));

		dataSetBuilder.append(ISACTIVE, true);

		if (dataSet.getVersion() != null) {
			dataSetBuilder.append("version", dataSet.getVersion());
		}

		return dataSetBuilder.get();

	}

	public static DBObject deleteDBObject(String datasetKey) {
		BasicDBObjectBuilder dataSetBuilder = BasicDBObjectBuilder.start();

		dataSetBuilder.append("_id", datasetKey);

		if (Utilities.isHardDeleteTurnedOn()) {
			dataSetBuilder.append(DATASETKEY, datasetKey);
		} else {
			dataSetBuilder.append(ISACTIVE, false);
		}

		return dataSetBuilder.get();
	}

	public static BasicDBObjectBuilder checkForDatasetDatasource(String user, String datasetKey, String datasourceKey)
			throws CmlpDataSrcException {

		BasicDBObjectBuilder query = BasicDBObjectBuilder.start();

		query.add("_id", datasetKey);
		query.add(OWNEDBY, user);
		query.add(ISACTIVE, true);
		query.add(DATASOURCEKEY, datasourceKey);

		return query;
	}

	public static DBObject updateAttributeMetaData( String datasourceKey,
			DatasetAttributeMetaData attributeMetaData) {
		BasicDBObjectBuilder dataSetBuilder = BasicDBObjectBuilder.start();

		if (datasourceKey != null) {
			dataSetBuilder.append(DATASOURCEKEY, datasourceKey);
		}
		setAttributeMetaData(attributeMetaData, dataSetBuilder);
		return dataSetBuilder.get();
	}
}
