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

package org.acumos.datasource.service;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.SocketAddress;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response.Status;
import javax.net.ssl.HttpsURLConnection;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import org.slf4j.LoggerFactory;
import org.acumos.datasource.exception.CmlpDataSrcException;
import org.acumos.datasource.common.CmlpApplicationEnum;
import org.acumos.datasource.common.CmlpErrorList;
import org.acumos.datasource.common.CmlpRestError;
import org.acumos.datasource.common.ErrorListEnum;
import org.acumos.datasource.common.HelperTool;
import org.acumos.datasource.common.Utilities;
import org.acumos.datasource.connection.DbUtilitiesV2;
import org.acumos.datasource.model.FileConnectionModel;
import org.acumos.datasource.schema.DataSourceModelGet;
import org.acumos.datasource.schema.DataSourceMetadata;
import org.acumos.datasource.schema.NameValue;
import org.slf4j.Logger;

@Service
public class FileDataSourceSvcImpl implements FileDataSourceSvc {

	private static Logger log = LoggerFactory.getLogger(FileDataSourceSvcImpl.class);

	public FileDataSourceSvcImpl() {
	}

	@Autowired
	private DbUtilitiesV2 dbUtilities;

	@Override
	public URLConnection getURLConnection(String fileURL, String username, String password)
			throws MalformedURLException, IOException, CmlpDataSrcException {
	
		log.info("connection url for file: " + fileURL);
		URL url = new URL(fileURL);
		URLConnection urlConn;

		// userid/password to access the service
		String authorization = null;
		String authStrEnc = null;

		if (username != null && username.length() > 0 && password != null && password.length() > 0) {
			authorization = username + ":" + password;
		}

		if (authorization != null) {
			authStrEnc = Base64.getEncoder().encodeToString(authorization.getBytes());
		}

		if(url.getProtocol().equalsIgnoreCase("file")){
			urlConn = url.openConnection();
			if (authStrEnc != null) {
				urlConn.setRequestProperty("Authorization", "Basic " + authStrEnc);
			}
			return urlConn;
		}
		
		//set the proxy
		SocketAddress addr = new InetSocketAddress("one.proxy.att.com", 8080);
		Proxy proxy = new Proxy(Proxy.Type.HTTP, addr);
		urlConn = url.openConnection(proxy);

		if (authStrEnc != null) {
			urlConn.setRequestProperty("Authorization", "Basic " + authStrEnc);
       }

       urlConn.setDoInput(true); // intend to use the URL connection for input only

       return urlConn;
	}
	
	@Override
	public String getConnectionStatus(FileConnectionModel objFileConnectionModel) throws CmlpDataSrcException {
		String fileURL = objFileConnectionModel.getFileURL();
		log.info("getConnectionStatus, trying to establish File connection with credentials using username: "
				+ objFileConnectionModel.getUsername());
		log.info("getConnectionStatus, trying to establish File connection with credentials for URL: "
				+ fileURL);		   
		String connectionStatus = "failed";
		
	    try {
			URLConnection conn = getURLConnection( objFileConnectionModel.getFileURL(),  objFileConnectionModel.getUsername(),  objFileConnectionModel.getPassword());
			if(conn.getURL().getProtocol().equalsIgnoreCase("file"))
			{			
				return "success";
			}
			
			//getting metaData
			ArrayList<NameValue> metaDataList = new ArrayList<NameValue>();
		 	String filename = "";
	        String disposition = conn.getHeaderField("Content-Disposition");
	        //we extract the file name either from the HTTP header Content-Disposition (in case the URL is an indirect link), or from the URL itself (in case the URL is the direct link).			       
	        if (disposition != null) {
	            // extracts file name from header field
	            int index = disposition.indexOf("filename=");
	            if (index > 0) {
	                filename = disposition.substring(index + 10,
	                        disposition.length() - 1);
	            }
	        } else {
	            // extracts file name from URL
	            filename = fileURL.substring(fileURL.lastIndexOf("/") + 1,
	                    fileURL.length());
	        }
			metaDataList.add(new NameValue("fileName", filename));	        
			long contentLength = conn.getContentLengthLong(); //return the length, in bytes, of the body of the message. 
			metaDataList.add(new NameValue("fileLength", String.valueOf(contentLength)));
			DataSourceMetadata metadata = new DataSourceMetadata();
			metadata.setMetaDataInfo(metaDataList);
			objFileConnectionModel.setMetaData(metadata);
			//test connection
			if(fileURL.startsWith("https")) {
				HttpsURLConnection httpsConn = (HttpsURLConnection) conn;
				httpsConn.connect();
				if(httpsConn.getResponseCode() == HttpURLConnection.HTTP_OK || httpsConn.getResponseCode() == HttpURLConnection.HTTP_MOVED_PERM || httpsConn.getResponseCode() == HttpURLConnection.HTTP_MOVED_TEMP) {
					connectionStatus = "success";
				} else {
					log.info("getConnectionStatus, Error connecting fileURL: " + fileURL + " " + httpsConn.getResponseMessage());
				}
				
				httpsConn.disconnect();
				
			} else if(fileURL.startsWith("http")) {
				HttpURLConnection httpConn = (HttpURLConnection) conn;
				httpConn.connect();
				if(httpConn.getResponseCode() == HttpURLConnection.HTTP_OK || httpConn.getResponseCode() == HttpURLConnection.HTTP_MOVED_PERM || httpConn.getResponseCode() == HttpURLConnection.HTTP_MOVED_TEMP) {
					connectionStatus = "success";
				} else {
					log.info("getConnectionStatus, Error connecting fileURL: " + fileURL + " " + httpConn.getResponseMessage());
				}
				
				httpConn.disconnect();
				
			} else {
		        InputStream inputStream = conn.getInputStream();
			    inputStream.close();
				connectionStatus = "success";
			}
		} catch (Exception e) {
			
		}		    
		 return connectionStatus;
	}

	@Override
	public InputStream getResults(String user, String authorization, String namespace, String datasourceKey)
				throws CmlpDataSrcException, IOException {
		
		URLConnection fileUrlConn = getConnection(user,  authorization,  namespace,  datasourceKey);
	    return fileUrlConn.getInputStream();
	}
	
	@Override
	public InputStream getSampleResults(String user, String authorization, String namespace, String datasourceKey)
				throws CmlpDataSrcException, IOException {
		URLConnection fileUrlConn = getConnection(user,  authorization,  namespace,  datasourceKey);	
	    InputStream inputStream = fileUrlConn.getInputStream();		        
		// Read the response
		BufferedReader br = new BufferedReader(new 
				InputStreamReader(inputStream));    
		String line;
		StringBuilder resultString = new StringBuilder();
		int rows=0, rowsLimit; 
		String strRowsLimit = HelperTool.getEnv("datasource_sample_size",
				HelperTool.getComponentPropertyValue("datasource_sample_size"));
		rowsLimit = strRowsLimit != null ? Integer.parseInt(strRowsLimit) : 5;
		
		while ((line = br.readLine()) != null && rows++<rowsLimit) {
			resultString = resultString.append(line).append("\r\n");;
		}
		inputStream.close();
		br.close();   	
		return new ByteArrayInputStream(resultString.toString().getBytes());
	}

	@Override
	public URLConnection getConnection(String user, String authorization, String namespace, String datasourceKey)
			throws MalformedURLException, IOException, CmlpDataSrcException {
		
		List<String> dbDatasourceDetails = dbUtilities.getDataSourceDetails(user, null, null, datasourceKey, null, true, false, authorization);
		
		DataSourceModelGet dbDataSource = Utilities.getDataSourceModel(dbDatasourceDetails.get(0));
		
		if (dbDataSource.getCategory().equals("file") && dbDataSource.getOwnedBy().equals(user)) {

			// initializing and populating a map that has key value pair for
			// credentials
			Map<String, String> decryptionMap = new HashMap<>();
			//decryptionMap = Utilities.readFromCodeCloud(authorization, datasourceKey);
			decryptionMap = Utilities.readFromMongoCodeCloud(user, datasourceKey);
			
			log.info("getConnection, using credetials, initializing file http connection");
		    return getURLConnection(dbDataSource.getFileDetails().getFileURL(),  decryptionMap.get("dbServerUsername".toLowerCase()),  decryptionMap.get("dbServerPassword".toLowerCase()));
		} else {
			//please check user permission and datasourcekey
			String[] variables = {"datasourceKey"};
			CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
			
			throw new CmlpDataSrcException(
					"please check datasource key provided and user permission for this operation", Status.NOT_FOUND.getStatusCode(), err);
		}
	}	

	@Override
	public InputStream getSampleResults(DataSourceModelGet dataSource) throws MalformedURLException, IOException, CmlpDataSrcException {
		URLConnection fileUrlConn = getURLConnection( dataSource.getFileDetails().getFileURL(),  dataSource.getFileDetails().getFileServerUserName(),  dataSource.getFileDetails().getFileServerUserPassword());
	    InputStream inputStream = fileUrlConn.getInputStream();		
	    
	    return getFileSample(inputStream);

	}

	private InputStream getFileSample(InputStream inputStream) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));    
		String line;
		StringBuilder resultString = new StringBuilder();
		int rows=0, rowsLimit; 
		String strRowsLimit = HelperTool.getEnv("datasource_sample_size",
				HelperTool.getComponentPropertyValue("datasource_sample_size"));
		rowsLimit = strRowsLimit != null ? Integer.parseInt(strRowsLimit) : 5;
		
		while ((line = br.readLine()) != null && rows++<rowsLimit) {
			resultString = resultString.append(line).append("\r\n");
		}
		inputStream.close();
		br.close();   	
		return new ByteArrayInputStream(resultString.toString().getBytes());
	}	
	
	
	@Override
	public boolean writebackPrediction(String authorization, DataSourceModelGet dataSource, String data) throws CmlpDataSrcException {
		log.info("writebackPrediction :  starting");
		
		boolean status = false;
		
		if(!dataSource.getReadWriteDescriptor().equalsIgnoreCase("write")){
			String[] variables = {"readWriteDescriptor"};
			CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
			throw new CmlpDataSrcException(
					"Writeback Prediction Data can not be done when ReadWriteDescriptor is not 'write'.", Status.BAD_REQUEST.getStatusCode(), err);
		}
		
		OutputStreamWriter osw =null;
		URLConnection fileUrlConn =null;
		
		try {
			log.info("Getting connection URL");
			fileUrlConn = getURLConnection(dataSource.getFileDetails().getFileURL(), dataSource.getFileDetails().getFileServerUserName(), dataSource.getFileDetails().getFileServerUserPassword());
			fileUrlConn.setDoInput(false);
			fileUrlConn.setDoOutput(true);
			
			if (fileUrlConn.getURL().getProtocol().equalsIgnoreCase("file")){
				log.info("The URL is of protocol : FILE");
				File file=new File(fileUrlConn.getURL().getFile());
				osw = new OutputStreamWriter(new FileOutputStream(file),Charset.forName("UTF-8").newEncoder() );
			} else {
				log.info("The URL is of protocol : http/s");
				osw = new OutputStreamWriter(fileUrlConn.getOutputStream());
			}

			osw.write(data);
			osw.close();
			
			//cross check
			log.info("Getting connection URL to cross check the file is written.");
			fileUrlConn = getURLConnection(dataSource.getFileDetails().getFileURL(), dataSource.getFileDetails().getFileServerUserName(), dataSource.getFileDetails().getFileServerUserPassword());
			fileUrlConn.setDoInput(true);
			InputStream inputStream = fileUrlConn.getInputStream();
			BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));    
		
			log.info("Reading the file system to cross check.");
			String line = null;
			String line1 = null;
			while ((line1 = br.readLine()) != null) {
				line = line1;
			}
			
			inputStream.close();
			br.close();   	
			
			log.info("The contents of file ends with: " + line);
			
			if(line != null && data.trim().endsWith(line)) {
				log.info("The end of the file contents are matching with input. Assume the data is successfully written to the file system.");
			status=true; 
			} else {
				log.info("Data is not matching. Failed to write to file system. Throwing exception...");
				throw new Exception("Failed to write to file system: " + dataSource.getFileDetails().getFileURL());
			}
		
		} catch (CmlpDataSrcException cdse) {
			throw cdse;
			
		} catch (Exception e) {
			
		} finally {
			if(osw != null) {
				try {
					osw.close();
				} catch (IOException io) {
					
				}
			}
		}
		
		return status;
	}	
}
