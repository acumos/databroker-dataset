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

package org.acumos.service.common;

public enum ErrorListEnum {

	_0001 ("A Service Error has Occurred. Contact support team with this REF ID : %1"),
	_0002 ("Missing Mandatory Parameter(s) - %"),
	_0003 ("Invalid value for Parameter(s) - %"),
	_0004 ("Invalid value for parameter '%'. Valid values are - %"),
	_0005 ("Invalid Number of Attachments. Valid no. of attachemnst are - %"),
	_0006 ("Invalid format of Attachments. Valid formats are - %"),
	_0007 ("One or more invalid connection parameter(s) - %"),
	_1001 ("Unauthorized-Missing authorization parameter - %"),
	_1003 ("User doesn't have necessary authorization to perform this action. %"),
	_1004 ("No information available for this resource."),
	_1029 ("Too many requests - Request is already in processing.");
	
	
    private String errorMsg;
   
    ErrorListEnum(String msg) {
        this.errorMsg = msg;
    }
    
    public String getErrorMessage() {
		return errorMsg;
	}
    
}
