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

package org.acumos.service.model;

import java.io.InputStream;

public class ContentResponseModel {
	
	private boolean batch;
	private String totalNoOfBatches;
	private String currentNoOfBatch;
	private InputStream data;
	/**
	 * @return the batch
	 */
	public boolean isBatch() {
		return batch;
	}
	/**
	 * @param batch the batch to set
	 */
	public void setBatch(boolean batch) {
		this.batch = batch;
	}
	/**
	 * @return the totalNoOfBatches
	 */
	public String getTotalNoOfBatches() {
		return totalNoOfBatches;
	}
	/**
	 * @param totalNoOfBatches the totalNoOfBatches to set
	 */
	public void setTotalNoOfBatches(String totalNoOfBatches) {
		this.totalNoOfBatches = totalNoOfBatches;
	}
	/**
	 * @return the currentNoOfBatch
	 */
	public String getCurrentNoOfBatch() {
		return currentNoOfBatch;
	}
	/**
	 * @param currentNoOfBatch the currentNoOfBatch to set
	 */
	public void setCurrentNoOfBatch(String currentNoOfBatch) {
		this.currentNoOfBatch = currentNoOfBatch;
	}
	/**
	 * @return the data
	 */
	public InputStream getData() {
		return data;
	}
	/**
	 * @param data the data to set
	 */
	public void setData(InputStream data) {
		this.data = data;
	}
	
	

}
