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


import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.StringTokenizer;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.http.HttpResponse;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CmlpErrorList {

	private static Logger log = LoggerFactory.getLogger(CmlpErrorList.class);
	
	
	private static String getErrorMessage(ErrorListEnum errorCode) {
		
		return errorCode.getErrorMessage();
	}
	
	private CmlpErrorList() {
		
	}
	
	
	public static CmlpRestError buildError(ErrorListEnum error, String[] errorVariables, String userMessage, CmlpApplicationEnum app) {
		
		//errorId:	DSET-0001
		//message:	"A service error occurred. Error code is %1"
		//userMessage:	"Something went wrong, please try again"
		//variables:	["variable from the system"]
		
		CmlpRestError restError = new CmlpRestError();
		
		try {
			String errorCode = app.getApplicationType() + error.toString(); //DSET_0001
			errorCode = errorCode.replace("_", "-"); //DSET-0001
			
			String msg = getErrorMessage(error);
			if(msg == null)
				throw new Exception("No such error message defined for errorCode: " + error);
			
			restError.setErrorCode(errorCode);
			restError.setErrorMessage(buildErrorMessage(msg, errorVariables));
			restError.setUserMessage(userMessage);
			restError.setErrorVariables(errorVariables);
			
		} catch (Exception e) {
			return buildError(e, null, app);
		}

		return restError;
	}
	
	public static CmlpRestError buildError(Exception sysException, ErrorListEnum error, CmlpApplicationEnum app) {
		
		log.info("Building Dataset error for the exception : ");
		log.info(sysException.getMessage());
		
		if(error == null)
			error = ErrorListEnum._0001;
		
		if(app == null)
			app = CmlpApplicationEnum.DATASET;
		
		String errorCode = app.getApplicationType() + error.toString(); //DSET_0001
		
		errorCode = errorCode.replace("_", "-"); //DSET-0001
		
		CmlpRestError restError = new CmlpRestError();
		
		restError.setErrorCode(errorCode);
		
		String msg = getErrorMessage(error);
		
		if(msg == null)
			msg = "";
		
		restError.setErrorMessage(buildErrorMessage(msg, null));
		
		String[] errorVariables = new String[1];
		errorVariables[0] = getRefId(sysException);
		restError.setErrorVariables(errorVariables);
		
		return restError;
	}
	
	
	private static String buildErrorMessage(String message, String[] errorVariables) {
		
		if(errorVariables == null || message == null || message.length() == 0)
			return message;
		
		int count = 0;
		StringBuilder sb = new StringBuilder();
		
		while(message.startsWith("%")) {
			sb.append("%").append(String.valueOf(count++ + 1)).append(" ");
			message = message.substring(1).trim();
		}
		
		StringTokenizer st = new StringTokenizer(message, "%");
		
		while(st.hasMoreTokens()) {
			sb.append(st.nextToken());
			if(count < errorVariables.length) {
				sb.append("%").append(String.valueOf(count++ + 1));
			}	
		}
		
		while (count < errorVariables.length) {
			sb.append(", ").append("%").append(String.valueOf(count++ + 1));
		}
		
		return sb.toString();
	}
	
	private static String getRefId(Exception e) {
		
		String refId = String.valueOf(Calendar.getInstance().getTimeInMillis()) + String.valueOf(RandomUtils.nextInt());
		
		ByteArrayOutputStream baos = null;
		ByteArrayInputStream bais = null;
		
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-YYYY'T'HH:mm:ss");
			
			baos = new ByteArrayOutputStream();
		    PrintStream ps = new PrintStream(baos);
		    
		    e.printStackTrace(ps);
		    ps.flush();
		    ps.close();
		    
		    bais = new ByteArrayInputStream(baos.toByteArray());
	    	BufferedReader bufReader = new BufferedReader(new InputStreamReader(bais));
            String temp = null;
            StringBuilder sb = new StringBuilder();
            sb.append("======================= REF_ID: ").append(refId).append(" =======================\r\n");
            sb.append(sdf.format(Calendar.getInstance().getTime())).append("\r\n");
            sb.append("Reason : " + e.getMessage() + "\r\n");
            
            while((temp = bufReader.readLine()) != null){
                if(temp.indexOf("cmlpdatasrcmgr_service") > 0)
                	sb.append(temp).append("\r\n");
            }
            
            log.info(sb.toString());
			
			if(!Files.exists(Paths.get("./logs"))) {
				Files.createDirectory(Paths.get("./logs"));
			}
			
			//Write to log and Audit file
			File logFile = new File("./logs/sys_exceptions.log");
			
			try(FileWriter fileWriter = new FileWriter(logFile.getAbsoluteFile(), true)) {
				fileWriter.write(sb.toString());
				fileWriter.flush();
			}
			
		} catch(Exception exc) {
			exc.printStackTrace();
		} finally {
			try{
                if(baos != null) baos.close();
            } catch (Exception ex){ }
		 
            try{
                if(bais != null) bais.close();
            } catch (Exception ex){ }
		}
		
		return refId;
	}
	
	//This method is used to build the error structure from the error returned by another API call
	public static CmlpRestError buildError(String jsonStr, String backendName) {
		
		//errorId:	DSET-0001
		//message:	"A service error occurred. Error code is %1"
		//userMessage:	"Something went wrong, please try again"
		//variables:	["variable from the system"]
		
		CmlpRestError restError = new CmlpRestError();
		
		try {
			
			if(jsonStr == null) {
				throw new Exception("Received null response from the '" + backendName + "' API call : ");
			}
			
			JSONObject jsonObj = new JSONObject(jsonStr);
			
			if(jsonObj.has("errorId")) {
				restError.setErrorCode(jsonObj.getString("errorId"));
			}
			
			if(jsonObj.has("message")) {
				restError.setErrorMessage(jsonObj.getString("message"));
			}
			
			if(jsonObj.has("variables")) {
				JSONArray jsonArray = jsonObj.getJSONArray("variables");
				String[] variables = new String[jsonArray.length()];
				
				for(int i=0; i<jsonArray.length(); i++) {
					variables[i] = jsonArray.getString(i);
				}
				
				restError.setErrorVariables(variables);
			}
			
			if(jsonObj.has("userMessage")) {
				restError.setErrorMessage(jsonObj.getString("userMessage"));
			}
			
		} catch (Exception e) {
			Exception e1 = new Exception(jsonStr);
			return buildError(e1, null, CmlpApplicationEnum.DATASOURCE);
		}

		return restError;
	}
	
	
	//This method is to translate the backend error
	public static CmlpRestError getCmlpRestError(HttpResponse response, String backendNameURL) {
		try {
			BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
			
			StringBuilder sb = new StringBuilder();
			String line = "";
			while ((line = rd.readLine()) != null) {
				sb.append(line);
				sb.append("\n");
			}
			
			if(sb.toString().trim().length() == 0)
				throw new Exception ("NULL response from the backend : " + backendNameURL);
			
			return CmlpErrorList.buildError(sb.toString(), backendNameURL);
			
		} catch (Exception e) {
			log.info("Issue in handling Http response from backend call. " + e.getMessage());
			return CmlpErrorList.buildError(e, null, CmlpApplicationEnum.DATASOURCE);
		}
	}
}
