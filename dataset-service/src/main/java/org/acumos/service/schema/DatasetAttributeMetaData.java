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

package org.acumos.service.schema;

import java.util.ArrayList;
import java.util.List;

public class DatasetAttributeMetaData {
	
	protected List<String> columnNames;
	protected List<String> columnTypes;
	protected List<DatasetTransformationInfo> transformations; // key, value
	
	public DatasetAttributeMetaData() {
		super();
		this.columnNames = new ArrayList<>();
		this.columnTypes = new ArrayList<>();
		this.transformations = new ArrayList<>();
		
		init();
	}
	
	public void init() {
		this.columnNames.add("");
		this.columnTypes.add("");
		this.transformations.add(new DatasetTransformationInfo());
	}
	
	public List<String> getColumnNames() {
		return columnNames;
	}
	public void setColumnNames(List<String> columnNames) {
		this.columnNames = columnNames;
	}
	public List<String> getColumnTypes() {
		return columnTypes;
	}
	public void setColumnTypes(List<String> columnTypes) {
		this.columnTypes = columnTypes;
	}
	public List<DatasetTransformationInfo> getTransformations() {
		return transformations;
	}
	public void setTransformations(List<DatasetTransformationInfo> transformations) {
		this.transformations = transformations;
	}
}
