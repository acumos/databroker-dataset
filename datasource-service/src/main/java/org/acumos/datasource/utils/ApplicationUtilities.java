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

package org.acumos.datasource.utils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import org.json.JSONObject;
import org.apache.http.util.EntityUtils;

import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.acumos.datasource.common.CMLPCipherSuite;
import org.acumos.datasource.common.CmlpApplicationEnum;
import org.acumos.datasource.common.DataSrcErrorList;
import org.acumos.datasource.common.DataSrcRestError;
import org.acumos.datasource.common.ErrorListEnum;
import org.acumos.datasource.common.HelperTool;
import org.acumos.datasource.common.KerberosConfigInfo;
import org.acumos.datasource.connection.DbUtilitiesV2;
import org.acumos.datasource.enums.CategoryTypeEnum;
import org.acumos.datasource.enums.ReadWriteTypeEnum;
import org.acumos.datasource.exception.DataSrcException;
import org.acumos.datasource.model.KerberosLogin;
import org.acumos.datasource.schema.DataSourceModelGet;
import org.acumos.datasource.schema.NameValue;
import org.slf4j.Logger;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.BaseEncoding;
import com.mongodb.DBObject;

public class ApplicationUtilities {

	private static Logger log = LoggerFactory.getLogger(ApplicationUtilities.class);
	
	public static void createKerberosKeytab(KerberosLogin objKerberosLogin) throws IOException {

		if (objKerberosLogin.getKerberosKeyTabContent() != null) {
			// writing keytab to a file
			HelperTool.writeKeytab(objKerberosLogin.getKerberosLoginUser(),
					objKerberosLogin.getKerberosKeyTabContent());
			// writing kerberos details to a conf file
			HelperTool.updateKrb5Conf(objKerberosLogin.getKerberosLoginUser(), objKerberosLogin.getKerberosRealms(),
					objKerberosLogin.getKerberosKdc(), objKerberosLogin.getKerbersoAdminServer(),
					objKerberosLogin.getKerberosPasswordServer(), objKerberosLogin.getKerberosDomainName());
		}
	}

	public static void createKerberosKeytab(KerberosLogin objKerberosLogin, String kerberosKeyTabFileName,
			String kerberosConfigFileName) throws IOException {

		if (objKerberosLogin.getKerberosKeyTabContent() != null) {
			// writing keytab to a file
			HelperTool.writeKeytab(objKerberosLogin.getKerberosLoginUser(), objKerberosLogin.getKerberosKeyTabContent(),
					kerberosKeyTabFileName);
			// writing kerberos details to a conf file
			HelperTool.updateKrb5Conf(objKerberosLogin.getKerberosLoginUser(), kerberosConfigFileName);
		}
	}

	
	public static String getName(String namespace, String category, String serverName, String user) {
		
		StringBuilder sb = new StringBuilder();
		sb.append(category.replaceAll(" ", "_"))
						.append("_")
						.append(((user.indexOf("@") > 0) ? user.substring(0, user.indexOf("@")).replaceAll("-", "_")
								: user.replaceAll("-", "_")))
						.append("_")
						.append(((serverName.indexOf(".") > 0) ? serverName.substring(0, serverName.indexOf(".")).replaceAll("-", "_")
								: serverName.replaceAll("-", "_")))
						.append("_")
						.append(Instant.now().toEpochMilli());
		
		/*String name = category.replaceAll(" ", "_") + "_"
				+ ((user.indexOf("@") > 0) ? user.substring(0, user.indexOf("@")).replaceAll("-", "_")
						: user.replaceAll("-", "_"))
				+ "_"
				+ ((serverName.indexOf(".") > 0) ? serverName.substring(0, serverName.indexOf(".")).replaceAll("-", "_")
						: serverName.replaceAll("-", "_"))
				+ "_" + Instant.now().toEpochMilli();*/
		
		return sb.toString();
	}

	
	public static String getEncrypt(String authorization, String value)
			throws DataSrcException, IOException {

		HttpClient client = HttpClients.createDefault();

		// setting url on request
		HttpPost request = new HttpPost(HelperTool.getEnv("configmgmt_encrypt_url",
				HelperTool.getComponentPropertyValue("configmgmt_encrypt_url")));
		request.addHeader("Authorization", authorization);
		request.addHeader("Content-Type", "text/plain");

		// generating entity and setting it
		StringEntity entity = new StringEntity(value);
		request.setEntity(entity);

		// executing request
		HttpResponse response = client.execute(request);

		int responseCode = response.getStatusLine().getStatusCode();
		
		// reading the response
		log.info(
				"response from encryption service by config management is " + response.getStatusLine().getStatusCode());
		StringBuilder result = new StringBuilder();
		if (responseCode == 200) {
			// reading response content and appending it to String buffer
			BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

			String line = "";
			while ((line = rd.readLine()) != null) {
				result.append(line);
			}
			
			rd.close();
		} else {
			//throw new CmlpDataSrcException("Oops.Something went wrong with Encryption.");
			
			//Read message from ConfigMgmtServer response and build new exception
			DataSrcRestError err = DataSrcErrorList.getCmlpRestError(response, "ConfigMgmtServer @ " + request.getURI());
			
			throw new DataSrcException("Oops.Something went wrong while contacting ConfigMgmtServer. ConfigMgmtServer retuned: " + response.getStatusLine().getReasonPhrase(),
					responseCode, err);
		}

		try {
			request.releaseConnection();
		} catch (Exception e) {
			log.info("ignoring the exception during release connection of http request.");
		}

		return result.toString();
	}

	
	public static Map<String, String> getDecrypt(String authorization, String datasourceKey)
			throws ClientProtocolException, IOException, DataSrcException {

		// HttpClient client = new DefaultHttpClient();
		HttpClient client = HttpClients.createDefault();

		// setting url for decrypt operation
		HttpGet request = new HttpGet(HelperTool.getEnv("configmgmt_decrypt_url_prefix",
				HelperTool.getComponentPropertyValue("configmgmt_decrypt_url_prefix")) + datasourceKey
				+ HelperTool.getEnv("configmgmt_decrypt_url_suffix",
						HelperTool.getComponentPropertyValue("configmgmt_decrypt_url_suffix")));
		request.addHeader("Authorization", authorization);
		request.addHeader("Content-Type", "text/plain");

		// executing post request
		log.info("getDecrypt(), Calling ConfigMgmt to get credentials...");
		HttpResponse response = client.execute(request);

		// to store all decrypted credentials
		Map<String, String> decryptionMap = new HashMap<>();

		// reading response
		log.info("response from decryption service by config management is " + response.getStatusLine().getStatusCode()
				+ " for datasourcekey: " + datasourceKey);
		if (response.getStatusLine().getStatusCode() == 200) {
			log.info("getDecrypt(), Received OK response from ConfigMgmt.");
			BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

			// reading response and populating map
			String line = "";
			StringBuilder lastKey = new StringBuilder("void");
			while ((line = rd.readLine()) != null) {
				if (line.contains(":")) {
					String[] mapping = line.split(":");
					decryptionMap.put(mapping[0].toLowerCase(), mapping[1].trim());
					lastKey.setLength(0);
					lastKey.append(mapping[0].toLowerCase());
				} else {
					decryptionMap.put(lastKey.toString(), decryptionMap.get(lastKey.toString()).concat(line));
				}
			}
			rd.close();
		
		} else if (response.getStatusLine().getStatusCode() == 204 || response.getStatusLine().getStatusCode() == 404) {
			//Not found and return an empty
			log.info("getDecrypt(), No files found in codecloud.");
		} else {
			log.info("getDecrypt(), Received Bad response from ConfigMgmt. Throwing exception...");
			DataSrcRestError err = DataSrcErrorList.getCmlpRestError(response, "ConfigMgmtServer @ " + request.getURI());
			
			throw new DataSrcException("Oops.Something went wrong while contacting ConfigMgmtServer. ConfigMgmtServer retuned: " + response.getStatusLine().getReasonPhrase(),
					response.getStatusLine().getStatusCode(), err);
		}

		try {
			request.releaseConnection();
		} catch (Exception e) {
			log.info("ignoring the exception during release connection of http request.");
		}

		return decryptionMap;
	}

	
	public static String doEncryptDecryptCommit(String authorization, String codeCloudAuthorization,
			String datasourceKey, Map<String, String> writeValues, String mode)
			throws IOException {

		StringBuilder data = null;
		boolean isCreate = false;
		boolean isUpdate = false;

		if (mode != null && mode.equals("create"))
			isCreate = true;
		else if (mode != null && mode.equals("update"))
			isUpdate = true;

		// generating file name based on datasourcekey provided
		String filenamePrefix = HelperTool.getEnv("filename_prefix", HelperTool.getComponentPropertyValue("filename_prefix"));
		if(filenamePrefix == null)
			filenamePrefix = "com_att_cmlp_config-";
		
		String filenameSuffix = HelperTool.getEnv("filename_suffix", HelperTool.getComponentPropertyValue("filename_suffix"));
		if(filenameSuffix == null)
			filenameSuffix = ".properties";
		
		String fileName = filenamePrefix
				+ datasourceKey
				+ filenameSuffix;

		// generating file based on teh filename generated above
		File encryptionDir = new File(System.getProperty("user.dir") + System.getProperty("file.separator")
				+ HelperTool.getEnv("filepath_string", HelperTool.getComponentPropertyValue("filepath_string")));
		
		File encryptionFile = new File(System.getProperty("user.dir") + System.getProperty("file.separator")
				+ HelperTool.getEnv("filepath_string", HelperTool.getComponentPropertyValue("filepath_string"))
				+ System.getProperty("file.separator") + fileName);

		if (isCreate) {
			log.info("Creating File: " + encryptionFile.getAbsolutePath());
			// when registering a new datasource
			if (!encryptionDir.exists()) {
				encryptionDir.mkdir();
			}

			encryptionFile.createNewFile();
		}

		if (isCreate || isUpdate) {
			data = new StringBuilder();
			for (String key : writeValues.keySet()) {
				data.append(key);
				data.append(":{cipher}");
				data.append(writeValues.get(key));
				data.append("\n");
			}

			// writing encrypted values to a file
			FileWriter filwWriter = new FileWriter(encryptionFile.getAbsoluteFile());
			filwWriter.write(data.toString());
			filwWriter.flush();
			filwWriter.close();

			log.info("Check in the file to CodeCloud: ");
//			CodeCloudCheckin.Checkin(fileName, codeCloudAuthorization);
			log.info("Checked in the file to CodeCloud successfully...");

			// delete the file stored in encrypted folder
			log.info("Deleting the file from local folder... ");
			ApplicationUtilities.deleteEncryptedFile(fileName);

			return fileName;

		} else {

			return fileName;
		}
	}

	// Returns DatasourceModel from the given json string
	public static DataSourceModelGet getDataSourceModel(String jsonStr) {
		if (jsonStr == null)
			return null;

		DataSourceModelGet dataSource = null;
		ObjectMapper mapper = new ObjectMapper(); // Jackson's JSON marshaller
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);

		try {

			dataSource = mapper.readValue(jsonStr, DataSourceModelGet.class);

		} catch (IOException e) {
			log.error("Error occurred while loading the Json to DataSourceModel");
		}

		return dataSource;
	}

	
	public static String getKerberosFileName(String user, String origFileName) {

		StringBuilder sb = new StringBuilder();
		/*
		 * int lastIndex = origFileName.lastIndexOf("."); if(lastIndex > 0) {
		 * sb.append(origFileName.substring(0,
		 * lastIndex)).append("$").append(origFileName.substring(lastIndex +
		 * 1)); } else sb.append(origFileName);
		 */

		sb.append(origFileName);
		sb.append("_").append(String.valueOf(Instant.now().toEpochMilli()))
				.append(String.valueOf(RandomUtils.nextInt()));

		return sb.toString();
	}

	
	public static String getOriginalFileName(String kerberosFileName) {

		if (kerberosFileName.indexOf("_") > 0) {
			kerberosFileName = kerberosFileName.substring(0, kerberosFileName.lastIndexOf("_"));
		}

		return kerberosFileName;
	}

	public static KerberosConfigInfo getKerberosConfigInfo(String kerberosConfigFileName, String kerberosKeyTabFileName)
			throws DataSrcException, IOException {

		KerberosConfigInfo kerberosConfig = new KerberosConfigInfo();

		StringBuilder sb1 = new StringBuilder();
		sb1.append(System.getProperty("user.dir")).append(System.getProperty("file.separator"))
				.append(HelperTool.getEnv("kerberos_user_config_dir",
						HelperTool.getComponentPropertyValue("kerberos_user_config_dir"))) // "./kerberos"
				.append(System.getProperty("file.separator")).append(kerberosConfigFileName);

		StringBuilder sb2 = new StringBuilder();
		sb2.append(System.getProperty("user.dir")).append(System.getProperty("file.separator"))
				.append(HelperTool.getEnv("kerberos_user_config_dir",
						HelperTool.getComponentPropertyValue("kerberos_user_config_dir"))) // "./kerberos"
				.append(System.getProperty("file.separator")).append(kerberosKeyTabFileName);

		if (Files.notExists(Paths.get(sb1.toString())) || Files.notExists(Paths.get(sb2.toString()))) {
			//throw new CmlpDataSrcException(
			//		"No Kerberos configuration files found. Please make sure the config and keytab files have been uploaded.");
			log.info("No Kerberos configuration files found. Please make sure the config and keytab files have been uploaded.");
			String[] variables = {"kerberosConfigFileId", "kerberosKeyTabFileId"};
		
			DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
			
			throw new DataSrcException(
					"No Kerberos configuration files found. Please make sure the config and keytab files have been uploaded.", Status.BAD_REQUEST.getStatusCode(), err);
		}

		// Setting KeyTab
		File keyTabFile = new File(sb2.toString());
		int fileLength = (int) keyTabFile.length();
		BufferedInputStream keytabBufferReader = new BufferedInputStream(new FileInputStream(keyTabFile));
		byte[] keytabByteArray = new byte[fileLength];
		keytabBufferReader.read(keytabByteArray);
		keytabBufferReader.close();

		StringBuilder sb = new StringBuilder();

		// int content;

		Pattern pattern = Pattern
				.compile("^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)$");
		Matcher matcher = pattern.matcher(new String(keytabByteArray));

		if (!matcher.find()) {
			String brOutput = BaseEncoding.base64().encode(keytabByteArray);
			kerberosConfig.setKerberosKeyTabContent(brOutput);
		} else {
			kerberosConfig.setKerberosKeyTabContent(new String(keytabByteArray));
		}

		// Setting
		BufferedReader br = null;
		FileReader fr = null;

		try {

			fr = new FileReader(sb1.toString());
			br = new BufferedReader(fr);

			String sCurrentLine;
			String default_realm = null;
			while ((sCurrentLine = br.readLine()) != null) {
				sCurrentLine = sCurrentLine.trim();
				if (sCurrentLine.startsWith("default_realm")) {
					StringTokenizer st = new StringTokenizer(sCurrentLine, "=");
					String temp = st.nextToken();
					if (st.hasMoreTokens()) {
						default_realm = st.nextToken().trim(); // get
																// default_realm
						break;
					}
				}
			}

			if (default_realm == null) {
				//throw new CmlpDataSrcException(
				//		"Couldn't find kerberos realm information. Please provide default_realm in config file.");
				
				log.info("Couldn't find kerberos realm information. Please provide default_realm in config file.");
				
				String[] variables = {"kerberosConfigFileId"};
				
				DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
				
				throw new DataSrcException(
						"Couldn't find kerberos realm information. Please provide default_realm in config file.", Status.BAD_REQUEST.getStatusCode(), err);

			} else {
				kerberosConfig.setKerberosRealms(default_realm);
			}

			br.close();
			fr.close();

			// get default_ream details
			fr = new FileReader(sb1.toString());
			br = new BufferedReader(fr);
			ArrayList<NameValue> configProperties = new ArrayList<NameValue>();
			boolean isRealm = false;
			while ((sCurrentLine = br.readLine()) != null) {
				sCurrentLine = sCurrentLine.trim();
				if (sCurrentLine.startsWith(default_realm)) {
					isRealm = true;
					continue;
				}

				if (isRealm) { // Realm details
					if (sCurrentLine.startsWith("{")) {
						sCurrentLine = sCurrentLine.substring(1);
					}

					if (sCurrentLine.endsWith("}")) { // end of Realm details
						isRealm = false;
						if (sCurrentLine.length() > 1)
							sCurrentLine = sCurrentLine.substring(0, sCurrentLine.length() - 1);
					}

					if (!sCurrentLine.equals("}")) {
						configProperties.add(new NameValue(sCurrentLine));
					}

					if (!isRealm) {
						break;
					}
				}
			}

			br.close();
			fr.close();

			for (NameValue keyValue : configProperties) {
				if (keyValue.getName() != null && keyValue.getName().equalsIgnoreCase("kdc")) {
					kerberosConfig.setKerberosKdc(keyValue.getValue());
					continue;
				}

				if (keyValue.getName() != null && keyValue.getName().equalsIgnoreCase("kpasswd_server")) {
					kerberosConfig.setKerberosPasswordServer(keyValue.getValue());
					continue;
				}

				if (keyValue.getName() != null && keyValue.getName().equalsIgnoreCase("admin_server")) {
					kerberosConfig.setKerberosAdminServer(keyValue.getValue());
					continue;
				}
			}

			fr = new FileReader(sb1.toString());
			br = new BufferedReader(fr);

			boolean isDomainRealm = false;
			String domainName = null;
			while ((sCurrentLine = br.readLine()) != null) {
				sCurrentLine = sCurrentLine.trim();
				if (sCurrentLine.indexOf("domain_realm") > 0)
					isDomainRealm = true;

				if (isDomainRealm) {
					if (!sCurrentLine.startsWith(".") && sCurrentLine.endsWith(default_realm)) {
						NameValue temp = new NameValue(sCurrentLine);
						domainName = temp.getName();
						break;
					}
				}
			}

			br.close();
			fr.close();

			if (domainName != null) {
				kerberosConfig.setDomainName(domainName);
			}

			// Check for Kerberos Configuration details
			if (kerberosConfig.getDomainName() == null || kerberosConfig.getKerberosAdminServer() == null
					|| kerberosConfig.getKerberosKdc() == null
					// || kerberosConfig.getKerberosPasswordServer() == null
					|| kerberosConfig.getKerberosRealms() == null
					|| kerberosConfig.getKerberosKeyTabContent() == null) {

				log.info(kerberosConfig.toString());
				log.info("Missing Kerberos config information from uploaded files. Throwing Exception...");
				//throw new CmlpDataSrcException(
				//		"Couldn't find complete kerberos config and keytab information from uploaded files. Please upload config files in correct template.");
				log.info("Couldn't find complete kerberos config and keytab information from uploaded files. Please upload config files in correct template.");
				String[] variables = {"kerberosConfigFileId"};
				
				DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
				
				throw new DataSrcException(
						"Couldn't find complete kerberos config and keytab information from uploaded files. Please upload config files in correct template.", Status.BAD_REQUEST.getStatusCode(), err);

			}

			// Read contents in whole
			fr = new FileReader(sb1.toString());
			br = new BufferedReader(fr);
			sb = new StringBuilder();

			while ((sCurrentLine = br.readLine()) != null) {
				if (sCurrentLine.trim().length() > 0)
					sb.append(sCurrentLine.trim()).append("@@@"); // replace
																	// newline
																	// char
			}

			// Replace { } [ ] chars with Html encoded values
			kerberosConfig.setConfigFileContents(ApplicationUtilities.htmlEncoding(sb.toString()));

			br.close();
			fr.close();

		} catch (IOException e) {

			//e.printStackTrace();
			throw e;

		} finally {

			try {
				if (br != null)
					br.close();

				if (fr != null)
					fr.close();
			} catch (IOException ex) {
			}
		}

		return kerberosConfig;
	}

	// delete uploaded kerberos configuration files after successfully
	// registered datasource.
	// These files information sent to codecloud to store in encrypted form.
	// No use of these files to store in hard desk due to SPI information
	public static void deleteUserKerberosConfigFiles(String kerberosConfigFileName, String kerberosKeyTabFileName) {

		try {
			StringBuilder sb1 = new StringBuilder();
			sb1.append(System.getProperty("user.dir")).append(System.getProperty("file.separator"))
					.append(HelperTool.getEnv("kerberos_user_config_dir",
							HelperTool.getComponentPropertyValue("kerberos_user_config_dir"))) // "./kerberos"
					.append(System.getProperty("file.separator")).append(kerberosConfigFileName);

			StringBuilder sb2 = new StringBuilder();
			sb2.append(System.getProperty("user.dir")).append(System.getProperty("file.separator"))
					.append(HelperTool.getEnv("kerberos_user_config_dir",
							HelperTool.getComponentPropertyValue("kerberos_user_config_dir"))) // "./kerberos"
					.append(System.getProperty("file.separator")).append(kerberosKeyTabFileName);

			Files.deleteIfExists(Paths.get(sb1.toString()));
			Files.deleteIfExists(Paths.get(sb2.toString()));
		} catch (Exception e) {
			log.info("ignoring the exception during file delete.");
		}
	}

	// delete kereberos config files used a cache to make kerberos connections.
	// This method will be called only when there is a failure to restore the
	// files during restoration from code cloud
	public static void deleteUserKerberosCacheConfigFiles(String kerberosConfigFileName,
			String kerberosKeyTabFileName) {

		try {
			StringBuilder sb1 = new StringBuilder();
			sb1.append(System.getProperty("user.dir")).append(System.getProperty("file.separator"))
					.append(System.getProperty("file.separator")).append(kerberosConfigFileName);

			StringBuilder sb2 = new StringBuilder();
			sb2.append(System.getProperty("user.dir")).append(System.getProperty("file.separator"))
					.append(System.getProperty("file.separator")).append(kerberosKeyTabFileName);

			Files.deleteIfExists(Paths.get(sb1.toString()));
			Files.deleteIfExists(Paths.get(sb2.toString()));
		} catch (Exception e) {
			log.info("ignoring the exception during file delete.");
		}
	}

	// This method will be called to check if the files exists before
	// restoration from code cloud
	public static boolean isUserKerberosCacheConfigFilesExists(String kerberosConfigFileName,
			String kerberosKeyTabFileName) {
		boolean isExists = false;
		try {
			StringBuilder sb1 = new StringBuilder();
			sb1.append(System.getProperty("user.dir")).append(System.getProperty("file.separator"))
					.append(System.getProperty("file.separator")).append(kerberosConfigFileName);

			StringBuilder sb2 = new StringBuilder();
			sb2.append(System.getProperty("user.dir")).append(System.getProperty("file.separator"))
					.append(System.getProperty("file.separator")).append(kerberosKeyTabFileName);

			isExists = Files.exists(Paths.get(sb1.toString()));

			if (isExists) {
				isExists = Files.exists(Paths.get(sb2.toString()));
			}
		} catch (Exception e) {
			log.info("ignoring the exception during file delete.");
		}

		return isExists;
	}

	// delete encrypted files created after successfully stored the file in
	// codecloud
	// As these files are stored in codecloud, they are no longer needed
	public static void deleteEncryptedFile(String fileName) {
		try {
			StringBuilder sb = new StringBuilder();

			sb.append(System.getProperty("user.dir")).append(System.getProperty("file.separator"))
					.append(HelperTool.getEnv("filepath_string",
							HelperTool.getComponentPropertyValue("filepath_string")))
					.append(System.getProperty("file.separator")).append(fileName);

			Files.deleteIfExists(Paths.get(sb.toString()));

		} catch (Exception e) {
			log.info("ignoring the exception during file delete.");
		}
	}

	
	public static String getName(String namespace) {

		StringBuilder sb = new StringBuilder(namespace);
		sb.append("_").append(String.valueOf(Instant.now().toEpochMilli())).append("_").append(getRandom()); // 40
																												// chars

		return sb.toString();
	}

	private static String getRandom() {

		return String.valueOf(RandomUtils.nextLong());
	}

/*	public static String getFieldValueFromJsonString(String jsonStr, String fieldName) throws Exception { // Returns
																											// fieldname
																											// from
																											// Json
																											// string
																											// using
		JSONObject jsonObj = new JSONObject(jsonStr);

		return jsonObj.getString(fieldName);
	}*/

	public static boolean writeToKerberosFile(String filename, String contents) {
		boolean result = false;

		// Modify contents which were encoded during encryption

		try {
			StringBuilder sb = new StringBuilder();
			sb.append(System.getProperty("user.dir")).append(System.getProperty("file.separator"))
					.append(HelperTool.getEnv("kerberos_user_config_dir",
							HelperTool.getComponentPropertyValue("kerberos_user_config_dir"))) // "./kerberos"
					.append(System.getProperty("file.separator")).append(filename);

			File kerberosFile = new File(sb.toString());

			FileWriter fileWriter = new FileWriter(kerberosFile.getAbsoluteFile());
			fileWriter.write(ApplicationUtilities.htmlDecoding(contents));

			result = true;

			fileWriter.flush();
			fileWriter.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return result;
	}

	public static boolean writeToKerberosCacheFile(String filename, String contents) {
		boolean result = false;

		// Modify contents which were encoded during encryption

		try {
			StringBuilder sb = new StringBuilder();
			sb.append(System.getProperty("user.dir")).append(System.getProperty("file.separator")).append(filename);

			File kerberosFile = new File(sb.toString());

			FileOutputStream fos = new FileOutputStream(kerberosFile.getAbsoluteFile());

			/*
			 * Pattern pattern = Pattern.compile(
			 * "^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)$"
			 * ); Matcher matcher = pattern.matcher(contents);
			 */

			if (filename.contains("krb5")) {
				fos.write(ApplicationUtilities.htmlDecoding(contents).getBytes());
		//	} else if (BaseEncoding.base64().canDecode(contents)) {// matcher.find()){
		//		byte[] brOutput = BaseEncoding.base64().decode(contents);
		//		fos.write(brOutput);
			} else {
				fos.write(contents.getBytes());
			}

			result = true;

			fos.flush();
			fos.close();
		} catch (Exception e) {
			//e.printStackTrace();
			log.info("writeToKerberosCacheFile(), Exception occurred while writing to Kerberos Cache File: " + e.getMessage());
		}

		return result;
	}

	// Method to decode certain encrypted values for GET operation
	// The values should be extracted from the file in configuration management
	public static ArrayList<String> decodeDataSourceDetails(ArrayList<String> lstDataSource, String authorization) {
		ArrayList<String> results = new ArrayList<String>();
		String strDataSource = null;
		boolean isDecoded = false;

		if (lstDataSource == null || authorization == null) {
			return lstDataSource;
		}

		try {
			if (lstDataSource != null) {
				strDataSource = lstDataSource.get(0);
			}

			// convert mongodb string into datasource object
			log.info("decodeDataSourceDetails(), Converting mongoDb Json string into DataSourceModel...");
			DataSourceModelGet datasource = ApplicationUtilities.getDataSourceModel(strDataSource);

			if (datasource != null) {
				log.info("decodeDataSourceDetails(), Category : " + datasource.getCategory());
				// convert kerberos encrypted fields
				if (datasource.getCategory() != null
						&& (datasource.getCategory().equals("hive") || datasource.getCategory().equals("Spark on Yarn")
								|| datasource.getCategory().equals("hdfs"))) {

					// Extract actual value from ConfigMgmt
					log.info("decodeDataSourceDetails(), calling Decrypt to fetch encrypted values for datasourceKey : "
							+ datasource.getDatasourceKey());
					Map<String, String> decryptionMap = ApplicationUtilities.readFromCodeCloud(authorization,
							datasource.getDatasourceKey());

					datasource.getHdfsHiveDetails().setKerberosLoginUser(decryptionMap.get("KerberosLoginUser".toLowerCase()));

					log.info("decodeDataSourceDetails(), Setting kerberosLoginUser");
					// convert object into json string
					// ObjectMapper mapper = new ObjectMapper(); //Jackson's
					// JSON marshaller
					// strDataSource = mapper.writeValueAsString(datasource);
					strDataSource = ApplicationUtilities.replaceValueInJson(strDataSource, "kerberosLoginUser",
							datasource.getHdfsHiveDetails().getKerberosLoginUser());

					results.add(strDataSource);

					isDecoded = true;
				}
			}
		} catch (Exception e) {
			log.info("decodeDataSourceDetails(), Exception occurred while handling decrypt: " + e.getMessage());
			//e.printStackTrace();
		}

		if (isDecoded) {
			return results;
		} else {
			return lstDataSource;
		}
	}

	// The intention of this method is not to corrupt the complex input json
	// string to replace a field value
	// This method should be replaced with better way.
	public static String replaceValueInJson(String inStr, String fieldName, String value) {
		try {
			int index = 0;

			if ((index = inStr.indexOf(fieldName)) < 0)
				return inStr;

			StringBuilder sb = new StringBuilder();

			index = inStr.indexOf(":", index); // location of : after fieldname
			index = inStr.indexOf("\"", index); // location of first "

			sb.append(inStr.substring(0, index + 1));
			sb.append(value);

			index = inStr.indexOf("\"", index + 1); // location of second "

			sb.append(inStr.substring(index));

			return sb.toString();

		} catch (Exception e) {
			return inStr;
		}
	}

	public static String htmlEncoding(String inStr) {
		String str = inStr.replaceAll("\\;", "&#59;").replaceAll("\\:", "&#58;").replaceAll("\\[", "&#91;")
				.replaceAll("\\]", "&#93;").replaceAll("\\{", "&#123;").replaceAll("\\}", "&#125;");

		return str;
	}

	public static String htmlDecoding(String inStr) {
		String str = inStr.replaceAll("&#123;", "{").replaceAll("&#125;", "}").replaceAll("&#91;", "[")
				.replaceAll("&#93;", "]").replaceAll("&#58;", ":").replaceAll("&#59;", ";").replaceAll("@@@", "\n");
		return str;
	}

	// any post processing clean up process
	public static void postprocessCleanup(String inStr) {

	}

	
	public static boolean validateConnectionParameters(DataSourceModelGet dataSource) throws DataSrcException {
		boolean result = false;
		
		if (dataSource.getCategory().equals("hive")) {

			// Check for hive required parameters
			if (dataSource.getCommonDetails() == null || 
					dataSource.getHdfsHiveDetails() == null ||
						dataSource.getHdfsHiveDetails().getKerberosConfigFileId() == null ||
						dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId() == null ||
						dataSource.getHdfsHiveDetails().getKerberosLoginUser() == null ||
						dataSource.getCommonDetails().getServerName() == null ||
						dataSource.getCommonDetails().getPortNumber() == 0) {

				log.error("Datasource Model has missing mandatory information for category type: hive. throwing excpetion...");
				ArrayList<String> missedParameters = new ArrayList<String>();
				if(dataSource.getCommonDetails() == null)
					missedParameters.add("commonDetails");
				else if(dataSource.getHdfsHiveDetails() == null)
					missedParameters.add("hdfsHiveDetails");
				else {
					if(dataSource.getHdfsHiveDetails().getKerberosConfigFileId() == null)
						missedParameters.add("kerberosConfigFileId");
					if(dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId() == null)
						missedParameters.add("kerberosKeyTabFileId");
					if(dataSource.getHdfsHiveDetails().getKerberosLoginUser() == null)
						missedParameters.add("kerberosLoginUser");
					if(dataSource.getCommonDetails().getServerName() == null)
						missedParameters.add("serverName");
					if(dataSource.getCommonDetails().getPortNumber() == 0)
						missedParameters.add("portNumber");
				}
				
				String[] variables = new String[missedParameters.size()];
				
				for(int i=0; i<missedParameters.size(); i++) {
					variables[i] = missedParameters.get(i);
				}
				
				DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0002, variables, null, CmlpApplicationEnum.DATASOURCE);
				
				throw new DataSrcException(
						"Datasource Model has missing mandatory information. Please send all the required information.", Status.BAD_REQUEST.getStatusCode(), err);

			} else {
				return true;
			}
		} else if (dataSource.getCategory().equals("hive batch")) {
			if (dataSource.getCommonDetails() == null || 
					dataSource.getHdfsHiveDetails() == null ||
						dataSource.getHdfsHiveDetails().getKerberosConfigFileId() == null ||
						dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId() == null ||
						dataSource.getHdfsHiveDetails().getKerberosLoginUser() == null ||
						dataSource.getHdfsHiveDetails().getBatchSize() == null ||
						dataSource.getCommonDetails().getServerName() == null  ||
						dataSource.getCommonDetails().getPortNumber() == 0) {

				log.error("Datasource Model has missing mandatory information for category type: hive batch. throwing excpetion...");
				ArrayList<String> missedParameters = new ArrayList<String>();
				if(dataSource.getCommonDetails() == null)
					missedParameters.add("commonDetails");
				else if(dataSource.getHdfsHiveDetails() == null)
					missedParameters.add("hdfsHiveDetails");
				else {
					if(dataSource.getHdfsHiveDetails().getKerberosConfigFileId() == null)
						missedParameters.add("kerberosConfigFileId");
					if(dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId() == null)
						missedParameters.add("kerberosKeyTabFileId");
					if(dataSource.getHdfsHiveDetails().getKerberosLoginUser() == null)
						missedParameters.add("kerberosLoginUser");
					if(dataSource.getHdfsHiveDetails().getBatchSize() == null)
						missedParameters.add("batchSize");
					if(dataSource.getCommonDetails().getServerName() == null)
						missedParameters.add("serverName");
					if(dataSource.getCommonDetails().getPortNumber() == 0)
						missedParameters.add("portNumber");
				}
				
				String[] variables = new String[missedParameters.size()];
				
				for(int i=0; i<missedParameters.size(); i++) {
					variables[i] = missedParameters.get(i);
				}
				
				DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0002, variables, null, CmlpApplicationEnum.DATASOURCE);
				
				throw new DataSrcException(
						"Datasource Model has missing mandatory information. Please send all the required information.", Status.BAD_REQUEST.getStatusCode(), err);
			} else {
				return true;
			}
			
		} else if (dataSource.getCategory().equals("cassandra")) {
			if(dataSource.getCommonDetails() == null ||
					dataSource.getDbDetails() == null ||
						dataSource.getCommonDetails().getServerName() == null ||
						dataSource.getCommonDetails().getPortNumber() == 0 ||
						dataSource.getDbDetails().getDbServerUsername() == null ||
						dataSource.getDbDetails().getDbServerPassword() == null ||
						//dataSource.getDbDetails().getDatabaseName() == null ||
						dataSource.getDbDetails().getDbQuery() == null) {
							
				log.error("Datasource Model has missing mandatory information for category type: mongo. throwing excpetion...");
				ArrayList<String> missedParameters = new ArrayList<String>();
				if(dataSource.getCommonDetails() == null)
					missedParameters.add("commonDetails");
				else if(dataSource.getDbDetails() == null)
					missedParameters.add("dbDetails");
				else {
					if(dataSource.getDbDetails().getDbServerUsername() == null)
						missedParameters.add("dbServerUsername");
					if(dataSource.getDbDetails().getDbServerPassword() == null)
						missedParameters.add("dbServerPassword");
					if(dataSource.getDbDetails().getDatabaseName() == null)
						missedParameters.add("databaseName");
					if(dataSource.getDbDetails().getDbQuery() == null)
						missedParameters.add("dbQuery");
					if(dataSource.getCommonDetails().getServerName() == null)
						missedParameters.add("serverName");
					if(dataSource.getCommonDetails().getPortNumber() == 0)
						missedParameters.add("portNumber");
				}
				
				String[] variables = new String[missedParameters.size()];
				
				for(int i=0; i<missedParameters.size(); i++) {
					variables[i] = missedParameters.get(i);
				}
				
				DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0002, variables, null, CmlpApplicationEnum.DATASOURCE);
				
				throw new DataSrcException(
						"Datasource Model has missing mandatory information. Please send all the required information.", Status.BAD_REQUEST.getStatusCode(), err);

				
			} else {
				return true;
			}
			
		} else if (dataSource.getCategory().equals("mongo")) {
			if(dataSource.getCommonDetails() == null || 
					dataSource.getDbDetails() == null || 
						dataSource.getCommonDetails().getServerName() == null ||
						dataSource.getCommonDetails().getPortNumber() == 0 ||
						//dataSource.getDbDetails().getDbServerUsername() == null ||
						//dataSource.getDbDetails().getDbServerPassword() == null ||
						dataSource.getDbDetails().getDatabaseName() == null ||
						dataSource.getDbDetails().getDbQuery() == null ||
						dataSource.getDbDetails().getDbCollectionName() == null) {
			
				log.error("Datasource Model has missing mandatory information for category type: mongo. throwing excpetion...");
				ArrayList<String> missedParameters = new ArrayList<String>();
				if(dataSource.getCommonDetails() == null)
					missedParameters.add("commonDetails");
				else if(dataSource.getDbDetails() == null)
					missedParameters.add("dbDetails");
				else {
					//if(dataSource.getDbDetails().getDbServerUsername() == null)
					//	missedParameters.add("dbServerUsername");
					if(dataSource.getCommonDetails().getServerName() == null)
						missedParameters.add("serverName");
					if(dataSource.getCommonDetails().getPortNumber() == 0)
						missedParameters.add("portNumber");
					if(dataSource.getDbDetails().getDatabaseName() == null)
						missedParameters.add("databaseName");
					if(dataSource.getDbDetails().getDbQuery() == null)
						missedParameters.add("dbQuery");
					if(dataSource.getDbDetails().getDbCollectionName() == null)
						missedParameters.add("dbCollectionName");
				}
				
				String[] variables = new String[missedParameters.size()];
				
				for(int i=0; i<missedParameters.size(); i++) {
					variables[i] = missedParameters.get(i);
				}
				
				DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0002, variables, null, CmlpApplicationEnum.DATASOURCE);
				
				throw new DataSrcException(
						"Datasource Model has missing mandatory information. Please send all the required information.", Status.BAD_REQUEST.getStatusCode(), err);

			} else {
				return true;
			}
			
		} else if (dataSource.getCategory().equals("mysql")) {
			if(dataSource.getCommonDetails() == null || 
					dataSource.getDbDetails() == null || 
						dataSource.getCommonDetails().getServerName() == null ||
						dataSource.getCommonDetails().getPortNumber() == 0 ||
						dataSource.getDbDetails().getDbServerUsername() == null || 
						dataSource.getDbDetails().getDbServerPassword() == null ||
						dataSource.getDbDetails().getDatabaseName() == null ||
						dataSource.getDbDetails().getDbQuery() == null) {
			
				log.error("Datasource Model has missing mandatory information for category type: mysql. throwing excpetion...");
				ArrayList<String> missedParameters = new ArrayList<String>();
				if(dataSource.getCommonDetails() == null)
					missedParameters.add("commonDetails");
				else if(dataSource.getDbDetails() == null)
					missedParameters.add("dbDetails");
				else {
					if(dataSource.getDbDetails().getDbServerUsername() == null)
						missedParameters.add("dbServerUsername");
					if(dataSource.getDbDetails().getDbServerPassword() == null)
						missedParameters.add("dbServerPassword");
					if(dataSource.getDbDetails().getDatabaseName() == null)
						missedParameters.add("databaseName");
					if(dataSource.getDbDetails().getDbQuery() == null)
						missedParameters.add("dbQuery");
					if(dataSource.getCommonDetails().getServerName() == null)
						missedParameters.add("serverName");
					if(dataSource.getCommonDetails().getPortNumber() == 0)
						missedParameters.add("portNumber");
				}
				
				String[] variables = new String[missedParameters.size()];
				
				for(int i=0; i<missedParameters.size(); i++) {
					variables[i] = missedParameters.get(i);
				}
				
				DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0002, variables, null, CmlpApplicationEnum.DATASOURCE);
				
				throw new DataSrcException(
						"Datasource Model has missing mandatory information. Please send all the required information.", Status.BAD_REQUEST.getStatusCode(), err);

			} else {
				return true;
			}
			
		} else if (dataSource.getCategory().equals("Spark Standalone")) {
			
		} else if (dataSource.getCategory().equals("Spark on Yarn")) {

			// Check for hive required parameters
			if (dataSource.getCommonDetails() == null || 
					dataSource.getHdfsHiveDetails() == null || 
						dataSource.getHdfsHiveDetails().getKerberosConfigFileId() == null ||
						dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId() == null ||
						dataSource.getHdfsHiveDetails().getHdpVersion() == null ||
						dataSource.getHdfsHiveDetails().getKerberosLoginUser() == null ||
						dataSource.getCommonDetails().getServerName() == null ||
						dataSource.getCommonDetails().getPortNumber() == 0	
						//dataSource.getSparkYarnJar() == null ||
						//dataSource.getYarnRMAddress() == null ||
						//dataSource.getYarnRMPrincipal() == null
						) {

				log.error("Datasource Model has missing mandatory information for category type: Spark on Yarn. throwing excpetion...");
				ArrayList<String> missedParameters = new ArrayList<String>();
				if(dataSource.getCommonDetails() == null)
					missedParameters.add("commonDetails");
				else if(dataSource.getHdfsHiveDetails() == null)
					missedParameters.add("hdfsHiveDetails");
				else {
					if(dataSource.getHdfsHiveDetails().getKerberosConfigFileId() == null)
						missedParameters.add("kerberosConfigFileId");
					if(dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId() == null)
						missedParameters.add("kerberosKeyTabFileId");
					if(dataSource.getHdfsHiveDetails().getHdpVersion() == null)
						missedParameters.add("hdpVersion");
					if(dataSource.getHdfsHiveDetails().getKerberosLoginUser() == null)
						missedParameters.add("kerberosLoginUser");
					if(dataSource.getCommonDetails().getServerName() == null)
						missedParameters.add("serverName");
					if(dataSource.getCommonDetails().getPortNumber() == 0)
						missedParameters.add("portNumber");
				}
				
				String[] variables = new String[missedParameters.size()];
				
				for(int i=0; i<missedParameters.size(); i++) {
					variables[i] = missedParameters.get(i);
				}
				
				DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0002, variables, null, CmlpApplicationEnum.DATASOURCE);
				
				throw new DataSrcException(
						"Datasource Model has missing mandatory information. Please send all the required information.", Status.BAD_REQUEST.getStatusCode(), err);

			} else {
				return true;
			}
		} else if (dataSource.getCategory().equals("hdfs")) {
			
			if (dataSource.getCommonDetails() == null || 
					dataSource.getHdfsHiveDetails() == null || 
						dataSource.getHdfsHiveDetails().getKerberosConfigFileId() == null ||
						dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId() == null ||
						dataSource.getHdfsHiveDetails().getKerberosLoginUser() == null ||
						dataSource.getCommonDetails().getServerName() == null ) {
				
				log.error("Datasource Model has missing mandatory information for category type: hdfs. throwing excpetion...");
				ArrayList<String> missedParameters = new ArrayList<String>();
				if(dataSource.getCommonDetails() == null)
					missedParameters.add("commonDetails");
				else if(dataSource.getHdfsHiveDetails() == null)
					missedParameters.add("hdfsHiveDetails");
				else {
					if(dataSource.getHdfsHiveDetails().getKerberosConfigFileId() == null)
						missedParameters.add("kerberosConfigFileId");
					if(dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId() == null)
						missedParameters.add("kerberosKeyTabFileId");
					if(dataSource.getHdfsHiveDetails().getKerberosLoginUser() == null)
						missedParameters.add("kerberosLoginUser");
					if(dataSource.getCommonDetails().getServerName() == null)
						missedParameters.add("serverName");
				}
				
				String[] variables = new String[missedParameters.size()];
				
				for(int i=0; i<missedParameters.size(); i++) {
					variables[i] = missedParameters.get(i);
				}
				
				DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0002, variables, null, CmlpApplicationEnum.DATASOURCE);
				
				throw new DataSrcException(
						"Datasource Model has missing mandatory information. Please send all the required information.", Status.BAD_REQUEST.getStatusCode(), err);

			}
			
		} else if (dataSource.getCategory().equals("hdfs batch")) {
			if (dataSource.getCommonDetails() == null || 
					dataSource.getHdfsHiveDetails() == null ||
						dataSource.getHdfsHiveDetails().getKerberosConfigFileId() == null ||
						dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId() == null ||
						dataSource.getHdfsHiveDetails().getKerberosLoginUser() == null ||
						//dataSource.getHdfsHiveDetails().getBatchSize() == null ||
						dataSource.getCommonDetails().getServerName() == null 
						//dataSource.getCommonDetails().getPortNumber() == 0
						) {

				log.error("Datasource Model has missing mandatory information for category type: hdfs batch. throwing excpetion...");
				ArrayList<String> missedParameters = new ArrayList<String>();
				if(dataSource.getCommonDetails() == null)
					missedParameters.add("commonDetails");
				else if(dataSource.getHdfsHiveDetails() == null)
					missedParameters.add("hdfsHiveDetails");
				else {
					if(dataSource.getHdfsHiveDetails().getKerberosConfigFileId() == null)
						missedParameters.add("kerberosConfigFileId");
					if(dataSource.getHdfsHiveDetails().getKerberosKeyTabFileId() == null)
						missedParameters.add("kerberosKeyTabFileId");
					if(dataSource.getHdfsHiveDetails().getKerberosLoginUser() == null)
						missedParameters.add("kerberosLoginUser");
					if(dataSource.getCommonDetails().getServerName() == null)
						missedParameters.add("serverName");
				}
				
				String[] variables = new String[missedParameters.size()];
				
				for(int i=0; i<missedParameters.size(); i++) {
					variables[i] = missedParameters.get(i);
				}
				
				DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0002, variables, null, CmlpApplicationEnum.DATASOURCE);
				
				throw new DataSrcException(
						"Datasource Model has missing mandatory information. Please send all the required information.", Status.BAD_REQUEST.getStatusCode(), err);

			} else {
				return true;
			}
			
		} else if (dataSource.getCategory().equals("file")) {
			if(dataSource.getFileDetails() == null || 
					dataSource.getFileDetails().getFileURL() == null ) {
				
				log.error("Datasource Model has missing mandatory information for category type: file. throwing excpetion...");
				ArrayList<String> missedParameters = new ArrayList<String>();
				if(dataSource.getFileDetails() == null)
					missedParameters.add("fileDetails");
				else {
					if(dataSource.getFileDetails().getFileURL() == null)
						missedParameters.add("fileURL");
				}
				
				String[] variables = new String[missedParameters.size()];
				
				for(int i=0; i<missedParameters.size(); i++) {
					variables[i] = missedParameters.get(i);
				}
				
				DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0002, variables, null, CmlpApplicationEnum.DATASOURCE);
				
				throw new DataSrcException(
						"Datasource Model has missing mandatory information. Please send all the required information.", Status.BAD_REQUEST.getStatusCode(), err);

			} else {
				return true;
			}
			
		} else if (dataSource.getCategory().equals("jdbc")) {
			if(dataSource.getCommonDetails() == null || 
				dataSource.getDbDetails() == null || 
					dataSource.getDbDetails().getJdbcURL() == null ||
					dataSource.getDbDetails().getDbServerUsername() == null ||
					dataSource.getDbDetails().getDatabaseName() == null ||
					dataSource.getDbDetails().getDbQuery() == null) {
				
				log.error("Datasource Model has missing mandatory information for category type: jdbc. throwing excpetion...");
				ArrayList<String> missedParameters = new ArrayList<String>();
				if(dataSource.getCommonDetails() == null)
					missedParameters.add("commonDetails");
				else if(dataSource.getDbDetails() == null)
					missedParameters.add("dbDetails");
				else {
					if(dataSource.getDbDetails().getJdbcURL() == null)
						missedParameters.add("jdbcURL");
					if(dataSource.getDbDetails().getDbServerUsername() == null)
						missedParameters.add("dbServerUsername");
					if(dataSource.getDbDetails().getDatabaseName() == null)
						missedParameters.add("databaseName");
					if(dataSource.getDbDetails().getDbQuery() == null)
						missedParameters.add("dbQuery");
				}
				
				String[] variables = new String[missedParameters.size()];
				
				for(int i=0; i<missedParameters.size(); i++) {
					variables[i] = missedParameters.get(i);
				}
				
				DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0002, variables, null, CmlpApplicationEnum.DATASOURCE);
				
				throw new DataSrcException(
						"Datasource Model has missing mandatory information. Please send all the required information.", Status.BAD_REQUEST.getStatusCode(), err);

			} else {
				return true;
			}	
		}
			
		
		return result;
	}
	
	
	public static DataSourceModelGet validateInputRequest(DataSourceModelGet dataSource, String mode) throws DataSrcException {
		//validate required fields
		ArrayList<String> missedParameters = new ArrayList<String>();
		if(dataSource.getOwnedBy() == null)
			missedParameters.add("Authorization");
		
		if(!missedParameters.isEmpty()) {
			String[] variables = new String[missedParameters.size()];
			
			for(int i=0; i<missedParameters.size(); i++) {
				variables[i] = missedParameters.get(i);
			}
			
			DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._1001, variables, null, CmlpApplicationEnum.DATASOURCE);
			
			throw new DataSrcException(
					"Unauthorized-Missing authorization parameter. Please send all the required information.", Status.UNAUTHORIZED.getStatusCode(), err);

		}
		
		//validate required parameters
		if(dataSource.getCategory() == null) {
			missedParameters.add("category");
		}
		
		if(dataSource.getNamespace() == null) {
			missedParameters.add("namespace");
		}
		
		if(dataSource.getDatasourceName() == null) {
			missedParameters.add("datasourceName");
		}
		
		if(dataSource.getReadWriteDescriptor() == null) {
			missedParameters.add("readWriteDescriptor");
		}
		
		if(!missedParameters.isEmpty()) {
			String[] variables = new String[missedParameters.size()];
			
			for(int i=0; i<missedParameters.size(); i++) {
				variables[i] = missedParameters.get(i);
			}
			
			DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0002, variables, null, CmlpApplicationEnum.DATASOURCE);
			
			throw new DataSrcException(
					"Missing Mandatory Parameters. Please send all the required information.", Status.BAD_REQUEST.getStatusCode(), err);

		}
		
		//Validate the field values
		if(dataSource.getCategory() != null) {
			boolean isValid = false;
			
			for (CategoryTypeEnum categoryType : CategoryTypeEnum.values()) {
				if(categoryType.getCategoryType().equalsIgnoreCase(dataSource.getCategory())) {
					isValid = true;
					break;
				}
			}
			
			if(!isValid) {
				raiseInvalidEnumerationException("category", 400);
			}
		}
		
		if(dataSource.getReadWriteDescriptor() != null) {
			boolean isValid = false;
			
			for (ReadWriteTypeEnum readWriteType : ReadWriteTypeEnum.values()) {
				if(readWriteType.getReadWriteType().equalsIgnoreCase(dataSource.getReadWriteDescriptor())) {
					isValid = true;
					break;
				}
			}
			
			if(!isValid) {
				raiseInvalidEnumerationException("readWriteDescriptor", 400);
			}
		}
		
		//check for Empty usernames and Passwords
		if(dataSource.getDbDetails() != null && dataSource.getDbDetails().getDbServerUsername() != null && dataSource.getDbDetails().getDbServerUsername().isEmpty())
			dataSource.getDbDetails().setDbServerUsername(null);
		
		if(dataSource.getDbDetails() != null && dataSource.getDbDetails().getDbServerPassword() != null && dataSource.getDbDetails().getDbServerPassword().isEmpty())
			dataSource.getDbDetails().setDbServerPassword(null);
		
		if(dataSource.getFileDetails() != null && dataSource.getFileDetails().getFileServerUserName() != null && dataSource.getFileDetails().getFileServerUserName().isEmpty())
			dataSource.getFileDetails().setFileServerUserName(null);
		
		if(dataSource.getFileDetails() != null && dataSource.getFileDetails().getFileServerUserPassword() != null && dataSource.getFileDetails().getFileServerUserPassword().isEmpty())
			dataSource.getFileDetails().setFileServerUserPassword(null);
		
		//check for hive query ends with semicolon
		if(dataSource.getHdfsHiveDetails() != null && dataSource.getHdfsHiveDetails().getQuery() != null) {
			if(dataSource.getCategory().equals(CategoryTypeEnum._HIVE) 
					|| dataSource.getCategory().equals(CategoryTypeEnum._HIVEBATCH)) {
				while (dataSource.getHdfsHiveDetails().getQuery().trim().endsWith(";")) {
					//Remove semicolon
					String query = dataSource.getHdfsHiveDetails().getQuery().trim();
					query = query.substring(0, query.length() - 1);
					
					dataSource.getHdfsHiveDetails().setQuery(query);
				}
			}
		}
		
		return dataSource;
	}
	
	private static void raiseInvalidEnumerationException(String parameter, int exceptionType) throws DataSrcException {
		ArrayList<String> validValues = new ArrayList<String>();
		
		if("category".equals(parameter)) {
			
			validValues.add(parameter);
			
			/*for (CategoryTypeEnum categoryType : CategoryTypeEnum.values()) {
				validValues.add(categoryType.getCategoryType());
			}*/
			
			CategoryTypeEnum[] categories = CategoryTypeEnum.values();
			StringBuilder sb = new StringBuilder();
			for(int i=0; i<categories.length; i++) {
				if(i > 0) {
					sb.append(", ");
			}
				sb.append(categories[i].getCategoryType());
			}
			
			validValues.add(sb.toString());
			
		} else if("readWriteDescriptor".equals(parameter)) {
			
			validValues.add(parameter);
			
			/*for (ReadWriteTypeEnum readWriteType : ReadWriteTypeEnum.values()) {
				validValues.add(readWriteType.getReadWriteType());
			}*/
			
			ReadWriteTypeEnum[] readWriteTypes = ReadWriteTypeEnum.values();
			StringBuilder sb = new StringBuilder();
			for(int i=0; i<readWriteTypes.length; i++) {
				if(i > 0) {
					sb.append(", ");
				}
				sb.append(readWriteTypes[i].getReadWriteType());
			}
			
			validValues.add(sb.toString());
		}
		
		if(!validValues.isEmpty()) {
		
			String[] variables = new String[validValues.size()];
			
			for(int i=0; i<validValues.size(); i++) {
				variables[i] = validValues.get(i);
			}
			
			DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0004, variables, null, CmlpApplicationEnum.DATASOURCE);
			
			if(exceptionType == 400) {
				throw new DataSrcException(
					"DataSource has invalid category value. Please send the correct value.", Status.BAD_REQUEST.getStatusCode(), err);
			} else {
				throw new DataSrcException(
						"DataSource has invalid category value. Please send the correct value.", Status.NOT_FOUND.getStatusCode(), err);

			}
		}	
	}
	
	
	public static void raiseConnectionFailedException(String category) throws DataSrcException {
		ArrayList<String> missedParameters = new ArrayList<String>();
		
		if("hive".equals(category)) {
			missedParameters.add("kerberosConfigFileId");
			missedParameters.add("kerberosKeyTabFileId");
			missedParameters.add("kerberosLoginUser");
			missedParameters.add("serverName");
			missedParameters.add("portNumber");
			missedParameters.add("query");
			
		} else if("hive batch".equals(category)) {
			missedParameters.add("kerberosConfigFileId");
			missedParameters.add("kerberosKeyTabFileId");
			missedParameters.add("kerberosLoginUser");
			missedParameters.add("batchSize");
			missedParameters.add("serverName");
			missedParameters.add("portNumber");
			missedParameters.add("query");
			
		} else if("hdfs".equals(category)) {
			missedParameters.add("kerberosConfigFileId");
			missedParameters.add("kerberosKeyTabFileId");
			missedParameters.add("kerberosLoginUser");
			missedParameters.add("hdfsFoldername");
			missedParameters.add("serverName");
			missedParameters.add("portNumber");
			
		} else if("hdfs batch".equals(category)) {
			missedParameters.add("kerberosConfigFileId");
			missedParameters.add("kerberosKeyTabFileId");
			missedParameters.add("kerberosLoginUser");
			missedParameters.add("hdfsFoldername");
			missedParameters.add("serverName");
			missedParameters.add("portNumber");
			
		} else if("file".equals(category)) {
			missedParameters.add("fileURL");
			
		} else if("jdbc".equals(category)) {
			missedParameters.add("jdbcURL");
			missedParameters.add("dbServerUsername");
			missedParameters.add("databaseName");
			missedParameters.add("dbQuery");
			
		} else if("mongo".equals(category)) {
			missedParameters.add("dbServerUsername");
			missedParameters.add("databaseName");
			missedParameters.add("dbQuery");
			missedParameters.add("dbCollectionName");
			missedParameters.add("serverName");
			missedParameters.add("portNumber");
			
		} else if("mysql".equals(category)) {
			missedParameters.add("dbServerUsername");
			missedParameters.add("dbServerPassword");
			missedParameters.add("databaseName");
			missedParameters.add("dbQuery");
			missedParameters.add("serverName");
			missedParameters.add("portNumber");
			
		} else if("cassandra".equals(category)) {
			missedParameters.add("dbServerUsername");
			missedParameters.add("dbServerPassword");
			missedParameters.add("databaseName");
			missedParameters.add("dbQuery");
			missedParameters.add("serverName");
			missedParameters.add("portNumber");
			
		} else if("Spark Standalone".equals(category)) {
			
		} else if("Spark on Yarn".equals(category)) {
			missedParameters.add("kerberosConfigFileId");
			missedParameters.add("kerberosKeyTabFileId");
			missedParameters.add("hdpVersion");
			missedParameters.add("kerberosLoginUser");
			missedParameters.add("serverName");
			missedParameters.add("portNumber");
		
		}	
		
		
		if(!missedParameters.isEmpty()) {
			
			String[] variables = new String[missedParameters.size()];
			
			for(int i=0; i<missedParameters.size(); i++) {
				variables[i] = missedParameters.get(i);
			}
			
			DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0007, variables, null, CmlpApplicationEnum.DATASOURCE);
			
			throw new DataSrcException(
					"DataSource has invalid Connection parameters. Please send the correct value.", Status.BAD_REQUEST.getStatusCode(), err);
		}	
	}
	
	
	public static void validateInputParameter(String parameterName, String paramValue, boolean isQueryParam) throws DataSrcException, Exception {
		
		if("category".equals(parameterName)) {
			
			boolean isValid = false;
			
			for (CategoryTypeEnum categoryType : CategoryTypeEnum.values()) {
				if(categoryType.getCategoryType().equalsIgnoreCase(paramValue)) {
					isValid = true;
					break;
				}
			}
			
			if(!isValid) {
				if(isQueryParam) { //404 Not Found
					raiseInvalidEnumerationException(parameterName, 404);
				} else { //400 Bad Request
					raiseInvalidEnumerationException(parameterName, 400);
				}
			}
		}
	}
	
	//Method to trim semicolon for hive query
	public static String trimSemicolonAtEnd(String query) {
		if(query == null)
			return query;
		
		while (query.trim().endsWith(";")) {
			//Remove semicolon
			query = query.substring(0, query.length() - 1);
		}
		
		return query;
	}
	
	
	public static String getResponseContentType(String datasourceKey) {
		String contentType = "application/json";
		try {
			DbUtilitiesV2 dbUtilities = new DbUtilitiesV2();
			DBObject dbObject =  dbUtilities.getDataSourceDetailsByKey(datasourceKey);
			
			DataSourceModelGet dbDatasourceModel = getDataSourceModel(dbObject.toString());
			
			if(dbDatasourceModel.getCategory().equals(CategoryTypeEnum._FILE.getCategoryType())) {
				String fileformat = dbDatasourceModel.getFileDetails().getFileFormat();
				
				if(fileformat != null) {
					fileformat = fileformat.trim().toLowerCase();
				} else {
					//derive it from URL
					String fileURL = dbDatasourceModel.getFileDetails().getFileURL();
					
					if(fileURL.endsWith(".txt") || fileURL.indexOf(".txt?") > 0)
						fileformat = ".txt";
					else if(fileURL.endsWith(".csv") || fileURL.indexOf(".csv?") > 0)
						fileformat = ".csv";
					else if(fileURL.endsWith(".html") || fileURL.indexOf(".html?") > 0)
						fileformat = ".html";
				}
				
				if(fileformat.endsWith("csv")) {
					contentType = "text/csv";
				} else if(fileformat.endsWith("txt")) {
					contentType = "text/plain";
				} else if(fileformat.endsWith("xml")) {
					contentType = "application/xml";
				} else if(fileformat.endsWith("html")) {
					contentType = "text/html";
				}
			} else if(dbDatasourceModel.getCategory().equals(CategoryTypeEnum._HIVE.getCategoryType())
						|| dbDatasourceModel.getCategory().equals(CategoryTypeEnum._HIVEBATCH.getCategoryType())
						|| dbDatasourceModel.getCategory().equals(CategoryTypeEnum._HDFS.getCategoryType())
						|| dbDatasourceModel.getCategory().equals(CategoryTypeEnum._HDFSBATCH.getCategoryType())) {
				
				contentType = "text/plain";
			}
					
		} catch (Exception e) {
			
		}
		
		return contentType;
	}
	
	public static InputStream getInputStream(FSDataInputStream response) {
	
		try {
			InputStream in = response.getWrappedStream();
			
			byte[] buff = new byte[8192];
	
	        int bytesRead = 0;
	
	        ByteArrayOutputStream bao = new ByteArrayOutputStream();
	
	        while((bytesRead = in.read(buff)) != -1) {
	           bao.write(buff, 0, bytesRead);
	        }
	
	        byte[] data = bao.toByteArray();
	
	        return (new ByteArrayInputStream(data));
		} catch (Exception e) {
			return null;
		}
	}
	
	
	//write to code cloud through ConfigManagementServer instead of SpringConfigServer
	public static String writeToCodeCloud1 (String authorization, String codeCloudAuthorization,
			String datasourceKey, Map<String, String> writeValues)
			throws DataSrcException, IOException {
	
		JSONObject jsonObj = new JSONObject();
		
		if(writeValues == null || writeValues.isEmpty()) {
			return null;
		} else {
			for (String key : writeValues.keySet()) {
				jsonObj.put(key, writeValues.get(key));
			}
		}
				
		// generating file name based on datasourcekey provided
		String fileName = HelperTool.getEnv("filename_prefix", "com_att_cmlp_config-")
				+ datasourceKey
				+ HelperTool.getEnv("filename_suffix", ".properties");
		
		HttpPut request = null;
		String responseStr = null;
	
		try {
			String configMgrURL = HelperTool.getEnv("configmanagement_base_url", HelperTool.getComponentPropertyValue("configmanagement_base_url")) + fileName;
			
			
			try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
	
				// populating request to fetch details about a datasource
				request = new HttpPut(configMgrURL);
				
				request.addHeader("Authorization", authorization);
				request.addHeader("codecloud-authorization", codeCloudAuthorization);
				request.addHeader("Content-Type", "application/json");
				request.addHeader("Accept", "*/*");
				
				StringEntity entity = new StringEntity(jsonObj.toString());
				request.setEntity(entity);
				
				log.info("request url for inserting credentials in codecloud through Configmanagement server: " + request.getURI());
		
				HttpResponse response = httpClient.execute(request);
				
				int responseCode = response.getStatusLine().getStatusCode();
				
				responseStr = EntityUtils.toString(response.getEntity());
	
				log.info("response code from Configmanagement server: "
						+ responseCode);
				log.info("response details from Configmanagement serverr: "
						+ response.getStatusLine().getReasonPhrase());
				
				if (responseCode == 200 || responseCode == 201) {
					return fileName;
				} else if (responseCode == 401 || responseCode == 403) {
					String[] variables = {"Authorization, CodeCloud-Authorization"};
					DataSrcRestError err = DataSrcErrorList.buildError(ErrorListEnum._0003, variables, null, CmlpApplicationEnum.DATASOURCE);
					
					throw new DataSrcException("Invalid Authorizations. Please send valid authorization information.",
							Status.BAD_REQUEST.getStatusCode(), err);
				} else {
					//Read message from Datasource response and build new exception
					DataSrcRestError err = DataSrcErrorList.buildError("Configmanagement server returns " + responseStr, configMgrURL);
					throw new DataSrcException("Oops.Something went wrong while contacting Configmanagement server. Configmanagement server retuned: " + responseStr,
							responseCode, err);
				}
			} //end of try-with-resources
		} catch (DataSrcException cmlpEx) {
			throw cmlpEx;
			
		} catch (Exception e) {
			DataSrcRestError err = DataSrcErrorList.buildError(e, null, CmlpApplicationEnum.DATASOURCE);
			throw new DataSrcException("Exception occurred while calling Configmanagement server.",
					Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
		} finally {
			if(request != null) {
				request.releaseConnection();
			}
		}
	}
	
	
	//write to mongo db instead of code cloud
	public static String writeToCodeCloudInMongo (String user,
			String datasourceKey, Map<String, String> writeValues, String mode)
			throws DataSrcException, IOException {
	
		Map<String, String> encryptionMap = new HashMap<String, String>();
		
		if(writeValues == null || writeValues.isEmpty()) {
			return null;
		} else {
			String value = null;
			
			CMLPCipherSuite cipherSuite = new CMLPCipherSuite(datasourceKey);
			
			for (String key : writeValues.keySet()) {
				value = writeValues.get(key);
				value = cipherSuite.encrypt(value);
				encryptionMap.put(key, value);
			}
		}
				
		// call DbUtilities to store encrypted info
		DbUtilitiesV2 dbUtilities = new DbUtilitiesV2();
		dbUtilities.insertCodeCloudCredentialsInMongo(user, datasourceKey, encryptionMap, mode);
		
		return datasourceKey;
	}
	
	//Read from code cloud through ConfigManagementServer instead of SpringConfigServer
	public static Map<String, String> readFromCodeCloud (String authorization,
			String datasourceKey)
			throws IOException {
		
		// Fetch the file name based on datasourcekey provided
		String fileName = HelperTool.getEnv("filename_prefix", HelperTool.getComponentPropertyValue("filename_prefix"))
				+ datasourceKey
				+ HelperTool.getEnv("filename_suffix", HelperTool.getComponentPropertyValue("filename_suffix"));
		
		HttpGet request = null;
		String responseStr = null;
		Map<String, String> decryptionMap = new HashMap<>(); // to store all decrypted credentials
		
		try {
			
			String configMgrURL = HelperTool.getEnv("configmanagement_base_url", HelperTool.getComponentPropertyValue("configmanagement_base_url")) + fileName;
			
			try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
				request = new HttpGet(configMgrURL);
				
				request.addHeader("Authorization", authorization);
				request.addHeader("Content-Type", "application/json");
				request.addHeader("Accept", "*/*");
				
				log.info("request url for inserting credentials in codecloud through Configmanagement server: " + request.getURI());
				log.info("getDecrypt(), Calling ConfigMgmt to get credentials...");
				
				HttpResponse response = httpClient.execute(request);
				
				int responseCode = response.getStatusLine().getStatusCode();
				
				log.info("response code from Configmanagement server: " + responseCode);
				log.info("response details from Configmanagement serverr: "
						+ response.getStatusLine().getReasonPhrase());
				
				if (responseCode == 200 || responseCode == 201) {
					log.info("getDecrypt(), Received OK response from ConfigMgmt.");
					responseStr = EntityUtils.toString(response.getEntity());
					
					JSONObject jsonObj = new JSONObject(responseStr);
					Iterator<String> keys = jsonObj.keys();
					
					String keyName = "";
					
					while(keys != null && keys.hasNext()) {
						keyName = keys.next();
						decryptionMap.put(keyName.toLowerCase(), jsonObj.getString(keyName));
					}
					
				} else if (response.getStatusLine().getStatusCode() == 204 || response.getStatusLine().getStatusCode() == 404) {
					//Not found and return an empty
					log.info("getDecrypt(), No files found in codecloud through ConfigManagementServer. Trying to fetch through SpringConfigServer...");
					decryptionMap = ApplicationUtilities.getDecrypt(authorization, datasourceKey);
				} else {
					log.info("getDecrypt(), Received Bad response from ConfigMgmt. Throwing exception...");
					//Read message from ConfigMgmtServer response and build new exception
					DataSrcRestError err = DataSrcErrorList.getCmlpRestError(response, "ConfigMgmtServer @ " + request.getURI());
					
					throw new DataSrcException("Oops.Something went wrong while contacting ConfigMgmtServer. ConfigMgmtServer retuned: " + response.getStatusLine().getReasonPhrase(),
							response.getStatusLine().getStatusCode(), err);
				}
			} //end of try-with-resources
		} catch (Exception e) {
			
		}

		try {
			if(request != null)
				request.releaseConnection();
		} catch (Exception e) {
			log.info("ignoring the exception during release connection of http request.");
		}

		return decryptionMap;
		
	}
	
	//read from mongo instead of code cloud
	public static Map<String, String> readFromMongoCodeCloud (String user,
			String datasourceKey)
			throws DataSrcException, IOException {
		
		DbUtilitiesV2 dbUtilities = new DbUtilitiesV2();
		
		return dbUtilities.getCredentialFromMongoCodeCloud(user, datasourceKey);
		
		
	}
	
	public static String getStringResponseFromInputStream(InputStream inStream) throws IOException {
		BufferedReader rd = new BufferedReader(new InputStreamReader(inStream));
		String line = "";
		StringBuilder sb = new StringBuilder();
		while ((line = rd.readLine()) != null) {
			sb.append(line);
		}

		rd.close();

		return sb.toString();
	}
}
