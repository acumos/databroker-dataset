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

import java.nio.charset.StandardCharsets;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import javax.ws.rs.core.Response.Status;

import org.acumos.datasource.exception.DataSrcException;

import com.google.common.io.BaseEncoding;
import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;

public class CMLPCipherSuite {
	
	private String gDatasourceKey; //global
	
	private static final String ALGORITHM = "AES";
	
	public CMLPCipherSuite(String datasourceKey) {
		if(datasourceKey != null && datasourceKey.length() > 16) {
			datasourceKey = datasourceKey.substring(datasourceKey.length() - 16, datasourceKey.length());
		}
		
		this.gDatasourceKey = datasourceKey;
	}
	
    public String encrypt(String inStr) throws DataSrcException {
    	try {
	        SecretKeySpec secretKey = new SecretKeySpec(gDatasourceKey.getBytes(), ALGORITHM);
	        Cipher cipher = Cipher.getInstance(ALGORITHM);
	        cipher.init(Cipher.ENCRYPT_MODE, secretKey);
	
	        byte[] encryptedBytes = cipher.doFinal(inStr.getBytes(StandardCharsets.UTF_8));
	        
	        //encode binary data into Standard and return
	        return BaseEncoding.base64().encode(encryptedBytes);
	        
	    } catch (Exception exc) {		
			CmlpRestError err = CmlpErrorList.buildError(exc, null, CmlpApplicationEnum.DATASOURCE);
			throw new DataSrcException("Exception occurred during Encryption of Credentials.",
					Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
		}
    }
    
    public String decrypt(String inStr) throws DataSrcException {
    	try {
	        SecretKeySpec secretKey = new SecretKeySpec(gDatasourceKey.getBytes(), ALGORITHM);
	        Cipher cipher = Cipher.getInstance(ALGORITHM);
	        cipher.init(Cipher.DECRYPT_MODE, secretKey);
	
	        //decode standard into binary
	        byte[] brOutput = BaseEncoding.base64().decode(inStr);
	        
	        byte[] decryptedBytes = cipher.doFinal(brOutput);
	        		
	        return new String(decryptedBytes);
	        
    	} catch (Exception exc) {		
			CmlpRestError err = CmlpErrorList.buildError(exc, null, CmlpApplicationEnum.DATASOURCE);
			throw new DataSrcException("Exception occurred during Decryption of Credentials.",
					Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
    	}
    }
}