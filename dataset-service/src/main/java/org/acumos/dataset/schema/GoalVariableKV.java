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

/**
 * @author am375y
 *
 */
public class GoalVariableKV implements Serializable{
	
private static final long serialVersionUID = 1L;
	
	//can be used in various scenarios like, sharedUser&Role, MetaData DetailName/Value, Any other KeyValue pairs as needed
	String name;
	String priority;
	
	public GoalVariableKV() {
	
	}
	
	public GoalVariableKV(String name, String priority) {
		this.name = name;
		this.priority = priority;
	}

	public String getName() {
		return name;
	}

	public void setName(String key) {
		this.name = key;
	}

	public String getPriority() {
		return priority;
	}

	public void setPriority(String value) {
		this.priority = value;
	}
	public boolean equals(Object obj) {
	    return (obj instanceof GoalVariableKV && ((GoalVariableKV) obj).priority == this.priority);
	}
	
	@Override
	public int hashCode() {
		return this.priority.hashCode();
	}
	
	public String toString() {
	    return "(" + name + ", " + priority + ")";
	}

}
