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

package org.acumos.dataset.exception;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.LoggerFactory;
import org.acumos.dataset.common.CmlpApplicationEnum;
import org.acumos.dataset.common.CmlpErrorList;
import org.acumos.dataset.common.CmlpRestError;
import org.slf4j.Logger;

/**
 * This class serves as a custom exception. 
 * @author am375y
 *
 */
public class DataSetException extends Exception {
	private static final long serialVersionUID = 1L;
	
	private static Logger log = LoggerFactory.getLogger(DataSetException.class);
	
	private int _code = Status.INTERNAL_SERVER_ERROR.getStatusCode();
	
	private CmlpRestError _err;
	
	private String _message;
	
	/**
	 * Default constructor
	 */
	public DataSetException() {
		super();
	}

	
	public DataSetException(String Message, int code, CmlpRestError error) {
		super(Message);
		this._message = Message;
		this._code = code;
		this._err = error;
	}

	public Response toResponse() {
		
		if(_err == null) {
			_err = CmlpErrorList.buildError(new Exception("Unknown Error"), null, CmlpApplicationEnum.DATASET);
		}
		
		log.info(_message);
		
		return Response.status(Status.fromStatusCode(_code)).entity(_err).build();
	}

}
