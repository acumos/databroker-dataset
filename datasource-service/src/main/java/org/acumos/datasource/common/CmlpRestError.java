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

package org.acumos.datasource.common;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CmlpRestError {
	private static final long serialVersionUID = 1L;
	
	//errorId:	DSET-0001
	//message:	"A service error occurred. Error code is %1"
	//userMessage:	"Something went wrong, please try again"
	//variables:	["variable from the system"]
	
	@JsonProperty("errorId")
	private String errorCode;
	
	@JsonProperty("message")
	private String errorMessage;
	
	@JsonProperty("userMessage")
	private String userMessage;
	
	@JsonProperty("variables")
	private String[] errorVariables;
	
	
	@JsonProperty("errorId")
	public void setErrorCode(String errorCode) {
		this.errorCode = errorCode;
	}
	
	@JsonProperty("message")
	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}
	
	@JsonProperty("userMessage")
	public void setUserMessage(String userMessage) {
		this.userMessage = userMessage;
	}
	
	@JsonProperty("variables")
	public void setErrorVariables(String[] errorVariables) {
		this.errorVariables = errorVariables;
	}
}
