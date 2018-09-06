/*-
 * ===============LICENSE_START=======================================================
 * Acumos
 * ===================================================================================
 * Copyright (C) 2018 AT&T Intellectual Property & Tech Mahindra. All rights reserved.
 * ===================================================================================
 * This Acumos software file is distributed by AT&T and Tech Mahindra
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

package org.acumos.datasource.controller;

import java.lang.invoke.MethodHandles;
import java.util.List;

import org.acumos.datasource.schema.FileDetailsInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Controller
@RequestMapping(value = "/"+DatasourceFileController.DATASOURCES_FILES, produces = MediaType.APPLICATION_JSON_VALUE)
public class DatasourceFileController {
	
	private static final String KEYTABFILE = "keytabfile";
	private static final String CONFIGFILE = "configfile";
	static final String DATASOURCES_FILES = "datasources/files";
	private static final String INTERNAL_SERVER_ERROR = "Internal Server Error";
	private static final String NOT_FOUND = "Not Found";
	private static final String FORBIDDEN = "Forbidden";
	private static final String UNAUTHORIZED = "Unauthorized";
	private static final String BAD_REQUEST = "Bad Request";
	private static final String OK = "OK";
	private static final String RETURNS_A_JSON_STRING_THAT_NOTIFIES_STATUS_OF_UPLOADING_FILES = "Returns a JSON string that notifies status of uploading files.";
	private static final String RESPOND_WITH_STATUS_OF_UPLOADING_KERBEROS_CONFIG_FILES = "Respond with status of uploading kerberos config files.";
	
	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	
	@RequestMapping(method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = RESPOND_WITH_STATUS_OF_UPLOADING_KERBEROS_CONFIG_FILES, notes = RETURNS_A_JSON_STRING_THAT_NOTIFIES_STATUS_OF_UPLOADING_FILES, response = FileDetailsInfo.class)
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = OK),
			@ApiResponse(code = 400, message = BAD_REQUEST),
			@ApiResponse(code = 401, message = UNAUTHORIZED),
			@ApiResponse(code = 403, message = FORBIDDEN),
			@ApiResponse(code = 404, message = NOT_FOUND),
			@ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR) })
	public FileDetailsInfo kerberosFileupload(
			@RequestParam(CONFIGFILE) List<RequestPart> bodyParts1,
			@RequestParam(KEYTABFILE) List<RequestPart> bodyParts2) throws Exception {
		
		return new FileDetailsInfo();
	}

}
