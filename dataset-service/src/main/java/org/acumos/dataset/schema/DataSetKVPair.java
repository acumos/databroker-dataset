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

package org.acumos.dataset.schema;

import java.io.Serializable;

public class DataSetKVPair implements Serializable {
	private static final long serialVersionUID = 1L;
	
	//can be used in various scenarios like, sharedUser&Role, MetaData DetailName/Value, Any other KeyValue pairs as needed
	String key;
	String value;
	
	public DataSetKVPair() {
		key = "";
		value = "";
	}
	
	public DataSetKVPair(String key, String value) {
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	public boolean equals(Object obj) {
	    return (obj instanceof DataSetKVPair && ((DataSetKVPair) obj).key == this.key);
	}
	
	@Override
	public int hashCode() {
		return this.key.hashCode();
	}
	
	public String toString() {
	    return "(" + key + ", " + value + ")";
	}
}
