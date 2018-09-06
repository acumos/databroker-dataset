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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Base64;
import java.util.Properties;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.Response.Status;

import org.acumos.datasource.exception.CmlpDataSrcException;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

import com.google.common.io.BaseEncoding;

import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.nio.file.StandardCopyOption;

/**
 * <p>
 * Helper class to maintain application level resources
 * </p>
 * 
 * @author mn461x
 * @since Feb 23, 2017
 * @version $Id$
 */
public class HelperTool {
	private static Logger log = LoggerFactory.getLogger(HelperTool.class);
	
	private static String _apiVersion;
	private static String _resourceURL;

	public static String getComponentPropertyValue(String key) throws IOException {
		try {
			Properties prop = new Properties();
			InputStream input;
			if (new File(System.getProperty("user.dir") + System.getProperty("file.separator") + "config.properties")
					.exists()) {
				input = new FileInputStream(
						System.getProperty("user.dir") + System.getProperty("file.separator") + "config.properties");
			} else {
				input = HelperTool.class.getResourceAsStream("/config.properties");
			}
			prop.load(input);
			input.close();
			return prop.getProperty(key);
		} catch (Exception e) { 
			log.info("Property not set either in environment variable or config.properties: " + key);
		}

		return "";
	}
	
	public static String getComponentPropertyValue(String key, String defaultValue) throws IOException {
		try {
			Properties prop = new Properties();
			InputStream input;
			String value = null;
			if (new File(System.getProperty("user.dir") + System.getProperty("file.separator") + "config.properties")
					.exists()) {
				input = new FileInputStream(
						System.getProperty("user.dir") + System.getProperty("file.separator") + "config.properties");
			} else {
				input = HelperTool.class.getResourceAsStream("/config.properties");
			}
			prop.load(input);
			input.close();
	
			value = prop.getProperty(key);
			
			if(value != null)
				return value;
			else
				return defaultValue;
		} catch (Exception e) { 
			log.info("Property not set either in environment variable or config.properties: " + key);
		}

		return "";
	}


	
	public static boolean isPath(String text) {

		if (text == null) {
			return false;
		}
		File inFile = new File(text);

		return !(!inFile.isDirectory() && !inFile.isFile());
	}

	
	public static boolean isFileExists(String text) throws IOException {

		if (text == null) {
			return false;
		}
		File inFile = null;
		if (text.toLowerCase().startsWith("file://")) {
			inFile = new File((new URL(text)).getFile());
		} else {
			inFile = new File(text);
		}

		return (inFile.exists());
	}

	
	public static boolean isFileUrl(String urlString) {
		try {
			new URL(urlString);
			return true;
		} catch (Exception ex) {
			return false;
		}
	}

	
	public static boolean isHDFS(String urlString) throws IOException {
		String[] schemes = { "hdfs", "swebhdfs", "webhdfs" };
		if (Arrays.asList(schemes).contains(urlString.split(":")[0])) {
			return true;
		} else {
			return false;
		}
	}

	
	public static String readHttpURLtoString(URL urlPath) throws IOException {
		String outcome = null;
		try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(urlPath.openStream()))) {
			StringBuilder stringBuilder = new StringBuilder();

			String inputLine;
			while ((inputLine = bufferedReader.readLine()) != null) {
				stringBuilder.append(inputLine);
				stringBuilder.append(System.lineSeparator());
			}
			outcome = stringBuilder.toString().trim();
		}
		return outcome;
	}

	
	public static void writeKeytab(String kerberosLoginuser, String content) throws IOException {
		File keyFile = new File(System.getProperty("user.dir") + System.getProperty("file.separator")
				+ extractUsername(kerberosLoginuser) + "." + "kerberos.keytab");
		keyFile.createNewFile();
		byte[] brOutput = BaseEncoding.base64().decode(content);
		try (FileOutputStream fos = new FileOutputStream(keyFile)) {
			fos.write(brOutput);
		}
	}
	
	
	public static void writeKeytab(String kerberosLoginuser, String content, String kerberosKeyTabFileName) throws IOException {
		StringBuilder sb1 = new StringBuilder();
		sb1.append(System.getProperty("user.dir")).append(System.getProperty("file.separator"))
			.append(extractUsername(kerberosLoginuser)).append(".kerberos.keytab");
		
		// File keyFile = new File(System.getProperty("user.dir") + System.getProperty("file.separator")
		//		+ extractUsername(kerberosLoginuser) + "." + "kerberos.keytab");
		
		File keyFile = new File(sb1.toString());
		keyFile.createNewFile();
		
		byte[] brOutput;
		
		try {
			brOutput = BaseEncoding.base64().decode(content);
			try (FileOutputStream fos = new FileOutputStream(keyFile)) {
				fos.write(brOutput);
			}
		} catch (Exception e) {
			//Binary Encrypted file
			//Copy the file as it is
			StringBuilder sb2 = new StringBuilder();
			sb2.append(System.getProperty("user.dir")).append(System.getProperty("file.separator"))
			  .append(HelperTool.getEnv("kerberos_user_config_dir", HelperTool.getComponentPropertyValue("kerberos_user_config_dir"))) // "./kerberos"
			  .append(System.getProperty("file.separator"))
			  .append(kerberosKeyTabFileName);
			
			Files.copy(Paths.get(sb2.toString()), Paths.get(sb1.toString()), StandardCopyOption.REPLACE_EXISTING);
			
			/*byte[] allBytes = Files.readAllBytes(Paths.get(sb2.toString()));
			String strOutput = BaseEncoding.base64().encode(allBytes);
			try (FileOutputStream fos = new FileOutputStream(keyFile)) {
				fos.write(strOutput);
			}*/
		}
	}

	
	public static void updateKrb5Conf(String kerberosLoginuser, String kerberosrealms, String kerberoskdc,
			String kerbersoadminserver, String kerberospasswordserver, String kerberosdomainname) throws IOException {
		File outputfilePath = new File(System.getProperty("user.dir") + System.getProperty("file.separator")
				+ extractUsername(kerberosLoginuser) + "." + "krb5.conf");
		outputfilePath.createNewFile();
		File inputfilePath = new File(
				System.getProperty("user.dir") + System.getProperty("file.separator") + "krb5.conf.template");
		String text = FileUtils.readFileToString(inputfilePath, "UTF-8");
		text = text.replaceAll("@default_realm@", kerberosrealms);
		text = text.replaceAll("@kdc@", kerberoskdc);
		text = text.replaceAll("@admin_server@", kerbersoadminserver);
		if(kerberospasswordserver != null) {
			text = text.replaceAll("@kpasswd_server@", kerberospasswordserver);
		}
		text = text.replaceAll("@domain_realm@", kerberosdomainname);
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputfilePath))) {
			writer.write(text);
			//writer.flush();
		}
	}
	
	public static void updateKrb5Conf(String kerberosLoginuser, String kerberosConfigFileName) throws IOException {
		StringBuilder sb1 = new StringBuilder();
		sb1.append(System.getProperty("user.dir")).append(System.getProperty("file.separator"))
			.append(extractUsername(kerberosLoginuser)).append(".krb5.conf");
		
		StringBuilder sb2 = new StringBuilder();
		sb2.append(System.getProperty("user.dir")).append(System.getProperty("file.separator"))
		  .append(HelperTool.getEnv("kerberos_user_config_dir", HelperTool.getComponentPropertyValue("kerberos_user_config_dir"))) // "./kerberos"
		  .append(System.getProperty("file.separator"))
		  .append(kerberosConfigFileName);
		
		Files.copy(Paths.get(sb2.toString()), Paths.get(sb1.toString()), StandardCopyOption.REPLACE_EXISTING);
	}

	
	public static String extractUsername(String userName) {
		return (userName.indexOf("@") > 0) ? userName.substring(0, userName.indexOf("@")) : userName;

	}

	public static boolean isFileinHttp(URL urlPath) {
		try {
			readHttpURLtoString(urlPath);
		} catch (Exception e) {
			return false;
		}
		return true;
	}

	
	public static boolean isHTTPServerOnline(String ipAddress, int port) {
		boolean b = true;
		try {
			InetSocketAddress sa = new InetSocketAddress(ipAddress, port);
			Socket ss = new Socket();
			ss.connect(sa, 1);
			ss.close();
		} catch (Exception e) {
			b = false;
		}
		return b;
	}

	public static String getRemoteUser(HttpServletRequest request)  throws CmlpDataSrcException {
		String user = null;
		
		if (request.getRemoteUser() != null) {
			return request.getRemoteUser();
		}
		
		if (request.getUserPrincipal() != null) {
			return request.getUserPrincipal().getName();
		}
		
		String authorization = request.getHeader("Authorization");
		if (authorization != null && authorization.startsWith("Basic")) {
			// Authorization: Basic base64credentials
			String base64Credentials = authorization.substring("Basic".length()).trim();
			String credentials = new String(Base64.getDecoder().decode(base64Credentials), Charset.forName("UTF-8"));
			// credentials = username:password
			final String[] values = credentials.split(":", 2);
			return values[0];
		}
		
		if(user == null) {
			String[] variables = { "Authorization" };
			
			CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum._1001, variables, null, CmlpApplicationEnum.DATASOURCE);
			
			throw new CmlpDataSrcException(
					"Unauthorized-Missing authorization parameter. Please send all the required information.", Status.UNAUTHORIZED.getStatusCode(), err);

		}
		
		return user;
	}

	public static String getEnv(String envKey, String defaultValue) {
		
		String value = System.getenv(envKey);
		
		if (value == null) {
			value = System.getProperty(envKey);
		}
		if (value == null) {
			value = defaultValue;
		}

		return value;
	}

	public static void setEnv(String envKey, String value) {
		if (value != null) {
			System.setProperty(envKey, value);
		}
	}

	
	public static String getJSONfromHttpResponse(String url, String path, String codeCloudAuthorization)
			throws IOException {
		Client client = ClientBuilder.newClient();
		WebTarget webResource = client.target(UriBuilder.fromUri(url).build());
		Response response = (Response) webResource.path(path).queryParam("limit", 1000).request().header("Authorization", codeCloudAuthorization)
				.get();
		JSONObject result = null;
		try {
			result = new JSONObject(response.readEntity(String.class));
		} catch (Exception ex) {
			throw new IOException("Invalid JSON file/format");
		}
		return result.toString();
	}

	public static String getJSONfromHttpResponse(String url, String path, String authorization,
			String codeCloudAuthorization) throws IOException {
		Client client = ClientBuilder.newClient();
		WebTarget webResource = client.target(UriBuilder.fromUri(url).build());
		Response response = (Response) webResource.path(path).request().header("Authorization", authorization)
				.header("CodeCloud-Authorization", codeCloudAuthorization).get();
		JSONObject result = null;
		try {
			result = new JSONObject(response.readEntity(String.class));
		} catch (Exception ex) {
			throw new IOException("Invalid JSON file/format");
		}
		return result.toString();
	}

	
	public static Response getResponsefromHttpResponse(String url, String path, String authorization,
			String codeCloudAuthorization) {
		Client client = ClientBuilder.newClient();
		WebTarget webResource = client.target(UriBuilder.fromUri(url).build());
		Response response = (Response) webResource.path(path).request().header("Authorization", authorization)
				.header("CodeCloud-Authorization", codeCloudAuthorization).get();

		return response;
	}

	
	public static byte[] getOutputStreamfromHttpResponse(String url, String path, String authorization,
			String codeCloudAuthorization, String destinationFileName) {
		Client client = ClientBuilder.newClient();
		WebTarget webResource = client.target(UriBuilder.fromUri(url).build());
		Response response = (Response) webResource.path(path).request().header("Authorization", authorization)
				.header("CodeCloud-Authorization", codeCloudAuthorization).get();

		byte[] outputStream = response.readEntity(byte[].class);
		return outputStream;
	}

	public static void unZipper(String inputZipFile, String unzipDir) throws IOException {
		// create output directory is not exists. If exists drop the folder and
		// recreate it.
		File outfolder = new File(unzipDir);
		if (outfolder.exists()) {
			cleanDirectory(outfolder);
		}
		outfolder.mkdir();

		ArchiveInputStream archInstream = null;
		try {
			final InputStream is = new FileInputStream(inputZipFile);
			archInstream = new ArchiveStreamFactory().createArchiveInputStream("zip", is);
			ZipArchiveEntry entry;
			while ((entry = (ZipArchiveEntry) archInstream.getNextEntry()) != null) {
				File currentFile = new File(unzipDir, entry.getName());
				if (entry.isDirectory()) {

					FileUtils.forceMkdir(currentFile);
				} else {

					// create parent dir if needed
					File parent = currentFile.getParentFile();
					if (!parent.exists()) {

						FileUtils.forceMkdir(parent);
					}

					OutputStream out = new FileOutputStream(currentFile);
					IOUtils.copy(archInstream, out);
					out.flush();
					IOUtils.closeQuietly(out);
				}
			}

			is.close();

		} catch (Exception e) {
			throw new IOException("Error extracting from " + inputZipFile + ". Error Message -" + e.getMessage());
		} finally {
			IOUtils.closeQuietly(archInstream);
		}

	}

	public static void removeDirectory(File directory) {
		if (directory.isDirectory()) {
			File[] files = directory.listFiles();
			if (files != null && files.length > 0) {
				for (File aFile : files) {
					removeDirectory(aFile);
				}
			}
			directory.delete();
		} else {
			directory.delete();
		}
	}

	public static void cleanDirectory(File directory) {
		if (directory.isDirectory()) {
			File[] files = directory.listFiles();
			if (files != null && files.length > 0) {
				for (File aFile : files) {
					removeDirectory(aFile);
				}
			}
		}
		directory.delete();
	}
	
	
	public static Configuration getKerberisedConfiguration(String hostName, String kerberosloginUser)
			throws IOException {
		//hostname without hdfs is a deprecated setting for hadoop
		if(!hostName.startsWith("hdfs://")) {
			hostName = "hdfs://" + hostName;
		}

		if(!hostName.endsWith("/")) {
			hostName = hostName + "/";
		}
		
		if (kerberosloginUser != null) {
			Configuration config = new Configuration();
			config.set("fs.defaultFS", hostName);
			config.set("hadoop.security.authentication", "Kerberos");
			config.set("dfs.datanode.kerberos.principal", kerberosloginUser);
			
			System.setProperty("java.security.krb5.conf",
					System.getProperty("user.dir") + System.getProperty("file.separator")
							+ kerberosloginUser.substring(0, kerberosloginUser.indexOf("@")) + "." + "krb5.conf");
			System.setProperty("javax.security.auth.useSubjectCredsOnly", "true");
			System.setProperty("dfs.datanode.kerberos.principal", kerberosloginUser);
			
			UserGroupInformation.setConfiguration(config);
			UserGroupInformation.loginUserFromKeytab(kerberosloginUser,
					System.getProperty("user.dir") + System.getProperty("file.separator")
							+ kerberosloginUser.substring(0, kerberosloginUser.indexOf("@")) + "." + "kerberos.keytab");
			return config;
		}
		
		return null;
	}
	
	

	
	public long calculateJdbcResultSizeBySampling(ResultSet results) throws SQLException, CmlpDataSrcException {
		log.info("calculateJdbcResultSizeBySampling()");

		ResultSetMetaData rsmd = results.getMetaData();
		int columnCount = rsmd.getColumnCount();

		// countRow
		int rowCount = getJdbcRowCount(results);
		
		int sampleSize = (int) Math.ceil(rowCount / 5.0);
		int batchSize = (int) Math.ceil(sampleSize / 3.0);
		int middle = (int) Math.ceil(rowCount / 2.0);

		log.info("Number of Records: " + rowCount);
		log.info("SamplingSize: " + sampleSize);
		log.info("BatchSize: " + batchSize);
		log.info("Middle rowNumber: " + middle);


		StringBuilder resultString = new StringBuilder();
		int rowNumber = 1;
		while (results.next()) {
			if (rowNumber == 1 + batchSize) {
				// System.out.println("rowNumber first batch END: " + rowNumber);
				results.absolute(middle);
				rowNumber = middle;
				log.info("\nMOVE CURSOR TO MIDDLE BATCH: " + rowNumber);
			}
			if (rowNumber == middle + batchSize) {
				// System.out.println("rowNumber middle batch END: " + rowNumber);
				results.absolute(rowCount - batchSize + 1);
				rowNumber = rowCount - batchSize + 1;
				log.info("MOVE CURSOR TO LAST BATCH: " + rowNumber);
			}

			if ((rowNumber >= 1 && rowNumber < 1 + batchSize) || (rowNumber >= middle && rowNumber < middle + batchSize)
					|| (rowNumber >= (rowCount - batchSize + 1) && rowNumber <= rowCount)) {
				for (int i = 1; i <= columnCount; i++) {
					if (i > 1) {
						resultString.append(",");
					}
					int type = rsmd.getColumnType(i);
					if (type == Types.VARCHAR || type == Types.CHAR || type == Types.LONGNVARCHAR) {
						resultString.append(results.getString(i));
					} else if (type == Types.BIT) {
						resultString.append(String.valueOf(results.getBoolean(i)));
					} else if (type == Types.BIGINT) {
						resultString.append(String.valueOf(results.getLong(i)));
					} else if (type == Types.NUMERIC || type == Types.DECIMAL) {
						resultString.append(String.valueOf(results.getBigDecimal(i)));
					} else if (type == Types.TINYINT || type == Types.SMALLINT || type == Types.INTEGER) {
						resultString.append(String.valueOf(results.getInt(i)));
					} else if (type == Types.REAL) {
						resultString.append(String.valueOf(results.getFloat(i)));
					} else if (type == Types.FLOAT || type == Types.DOUBLE) {
						resultString.append(String.valueOf(results.getDouble(i)));
					} else if (type == Types.BINARY || type == Types.LONGVARBINARY || type == Types.VARBINARY) {
						resultString.append(String.valueOf(results.getByte(i)));
					} else if (type == Types.DATE || type == Types.TIME || type == Types.TIMESTAMP) {
						resultString.append(String.valueOf(results.getTime(i)));
					} else if (type == Types.BLOB || type == Types.CLOB || type == Types.NCLOB) {
						return -1L;
					} else {
						//throw new CmlpDataSrcException("OOPS. Dev missed mapping for DATA Type. Please raise a bug.");
						CmlpRestError err = CmlpErrorList.buildError(new Exception("OOPS. Dev missed mapping for DATA Type.. Please raise a bug."), null, CmlpApplicationEnum.DATASOURCE);
						throw new CmlpDataSrcException("OOPS. Dev missed mapping. Please raise a bug.",
								Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);
					}
				} // for loop ends
			} // if expression ends
			resultString.append("\r\n");
			// log.info("rowNumber completed: " + rowNumber);
			rowNumber++;
		}
		log.info("\nPast Last Record: " + rowNumber);
		long size = 0;
		size = resultString.toString().getBytes().length * 1L;
		log.info("\nSize of 20% data: " + size + " bytes");
		return size * 5;
	}

	public long calculateJdbcResultSizeByNotSampling(ResultSet results) throws SQLException, CmlpDataSrcException {
		long size = 0L;
		log.info("calculateJdbcResultSizeByNotSampling()");

		ResultSetMetaData rsmd = results.getMetaData();
		int columnCount = rsmd.getColumnCount();
		// countRow
		int rowCount = getJdbcRowCount(results);
		log.info("Number of Records: " + rowCount);
		
		StringBuilder resultString = new StringBuilder();
		int rowNumber = 1;

		while (results.next()) {
			for (int i = 1; i <= columnCount; i++) {
				if (i > 1) {
					resultString.append(",");
				}
				int type = rsmd.getColumnType(i);
				if (type == Types.VARCHAR || type == Types.CHAR || type == Types.LONGNVARCHAR || type == Types.LONGVARCHAR) {
					resultString.append(results.getString(i));
				} else if (type == Types.BIT) {
					resultString.append(String.valueOf(results.getBoolean(i)));
				} else if (type == Types.BIGINT) {
					resultString.append(String.valueOf(results.getLong(i)));
				} else if (type == Types.NUMERIC || type == Types.DECIMAL) {
					resultString.append(String.valueOf(results.getBigDecimal(i)));
				} else if (type == Types.TINYINT || type == Types.SMALLINT || type == Types.INTEGER) {
					resultString.append(String.valueOf(results.getInt(i)));
				} else if (type == Types.REAL) {
					resultString.append(String.valueOf(results.getFloat(i)));
				} else if (type == Types.FLOAT || type == Types.DOUBLE) {
					resultString.append(String.valueOf(results.getDouble(i)));
				} else if (type == Types.BINARY || type == Types.LONGVARBINARY || type == Types.VARBINARY) {
					resultString.append(String.valueOf(results.getByte(i)));
				} else if (type == Types.DATE || type == Types.TIME || type == Types.TIMESTAMP) {
					resultString.append(String.valueOf(results.getTime(i)));
				} else if (type == Types.BLOB || type == Types.CLOB || type == Types.NCLOB) {
					return -1L;
				} else {
					return -1L;
					/*CmlpRestError err = CmlpErrorList.buildError(new Exception("OOPS. Dev missed mapping for DATA Type.. Please raise a bug."), null, CmlpApplicationEnum.DATASOURCE);
					throw new CmlpDataSrcException("OOPS. Dev missed mapping. Please raise a bug.",
							Status.INTERNAL_SERVER_ERROR.getStatusCode(), err);*/
				}
			}
			resultString.append("\r\n");
			// log.info("rowNumber completed: " + rowNumber);
			rowNumber++;
		}

		log.info("Past Last Record: " + rowNumber);
		size = resultString.toString().getBytes().length * 1L;
		log.info("\nSize of 100% dataset: " + size + " bytes");

		return size;
	}
	
	private int getJdbcRowCount(ResultSet results) throws SQLException {
		log.info("getJdbcRowCount()");
		int rowCount = 0;
		results.last();
		rowCount = results.getRow();

		results.beforeFirst();

		return rowCount;
	}
	

	
	public static String getAPIVersion(HttpServletRequest request) {
		if(_apiVersion == null) {
			
			try {
		
				String requestURI = request.getRequestURI(); // /cmlp/datasets/v2/datasets
				String cntxtPath = request.getContextPath(); // /cmlp/datasets
				
				String resourceURI = requestURI.substring(cntxtPath.length() + 1); // v2/datasets
	
				_apiVersion = resourceURI.substring(0, resourceURI.indexOf("/")); // v2
				
			} catch (Exception e) {
				_apiVersion = "v2";
			}
		}
		
		log.info("requestURL: " + request.getRequestURL());
		log.info("requestURI: " + request.getRequestURI());
		log.info("cntxtPath: " + request.getContextPath());
		log.info("PathInfo: " + request.getPathInfo());
		
		return _apiVersion;
	}
	
	public static String getAPIVersion() {
		
		return _apiVersion;
	}
	

	public static void  validateWriteBackDataSize(long dataSize)throws CmlpDataSrcException{
		long writeBackDataSizeLimit= 100;
		long writeBackDataSize = 0;
		try{
			log.info("validateWriteBackDataSize():");
			writeBackDataSizeLimit = Integer.parseInt(HelperTool.getEnv("writeBackDataSizeLimit",HelperTool.getComponentPropertyValue("writeBackDataSizeLimit")));
			//writeBackDataSize = data.getBytes().length/1024;
			writeBackDataSize = dataSize/1024;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
		if(writeBackDataSize > writeBackDataSizeLimit){
			String[] variables = {"writeBackDataSizeLimit="+writeBackDataSizeLimit+" in kb  as configured in config file or environment","Write back data size should be less than writeBackDataSizeLimit"};
			CmlpRestError err = CmlpErrorList.buildError(ErrorListEnum._0004, variables, null, CmlpApplicationEnum.DATASOURCE);
			throw new CmlpDataSrcException(
				"The actual size of the write back data should be less than  writeBackDataSizeLimit="+writeBackDataSizeLimit+" in kb as configured in config file or environment", Status.BAD_REQUEST.getStatusCode(), err);
		}
		log.info("validateWriteBackDataSize(): write back data size: " + writeBackDataSize + " in kb");
	}
	
	
	public static String getResourceURL(HttpServletRequest request) {
		try {
			if(_resourceURL == null) {
				String requestURL = request.getRequestURL().toString();
				String requestURI = request.getRequestURI().toString();
				
				String baseURL = requestURL.substring(0, requestURL.indexOf(requestURI));
				String ingress_path = HelperTool.getEnv("ingress_service_path",
						HelperTool.getComponentPropertyValue("ingress_service_path"));
				
				StringBuffer sb = new StringBuffer();
				sb.append(baseURL).append("/").append(ingress_path).append(requestURI);
				
				_resourceURL = sb.toString();
				
				log.info("requestURL: " + requestURL);
				log.info("requestURI: " + requestURI);
				log.info("baseURL: " + baseURL);
				log.info("ingress_path: " + ingress_path);
				log.info("ResourceURL: " + _resourceURL);		
			}
		} catch (Exception e) {
			e.printStackTrace();
			return request.getRequestURL().toString();
		}
		
		return _resourceURL;
	}
}
