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

package org.acumos.dataset.common;

public enum ErrorListEnum {
	
	E_0001 ("A Service Error has Occurred. Contact support team with this REF ID : %1"),
	E_0002 ("Missing Mandatory Parameter(s) - %"),
	E_0003 ("Invalid value for Parameter(s) - %"),
	E_0004 ("Invalid value for parameter '%'. Valid values are - %"),
	E_1001 ("Unauthorized-Missing authorization parameter - %"),
	E_1003 ("User doesn't have necessary authorization to perform this action. %"),
	E_1004 ("No information available for this resource."),
	E_1029 ("Too many requests - Request is already in processing.");
	
	
    private String errorMsg;
   
    ErrorListEnum(String msg) {
        this.errorMsg = msg;
    }
    
    public String getErrorMessage() {
		return errorMsg;
	}
    

}
