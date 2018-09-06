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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Properties;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response.Status;

import org.acumos.dataset.exception.DataSetException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Helper class to maintain application level resources
 * </p>
 */
public class HelperTool {

	private static Logger log = LoggerFactory.getLogger(HelperTool.class);

	private static String apiVersion;
	private static String resourceURL;
	private static final String MESSAGE = "getComponentPropertyValue(), trying to find value for property ";
	private static final String REMOTEMESSAGE = "getRemoteUser(), trying to find remote user for request ";
	
	private HelperTool() {
		super();
	}

	
	public static String getComponentPropertyValue(String key) throws IOException {
		log.info(MESSAGE + key);
		Properties prop = new Properties();
		InputStream input;
		if (new File(System.getProperty("user.dir") + System.getProperty("file.separator") + "config.properties")
				.exists()) {
			log.info(MESSAGE + key
					+ " from custom config file.");
			input = new FileInputStream(
					System.getProperty("user.dir") + System.getProperty("file.separator") + "config.properties");
		} else {
			log.info(MESSAGE + key
					+ " from provided config file.");
			input = HelperTool.class.getResourceAsStream("/config.properties");
		}
		prop.load(input);
		return prop.getProperty(key);
	}

	
	public static String getRemoteUser(HttpServletRequest request) throws DataSetException {
		log.info(REMOTEMESSAGE + request.getRequestedSessionId());
		if (request.getRemoteUser() != null) {
			log.info(REMOTEMESSAGE + request.getRequestedSessionId() + " as "
					+ request.getRemoteUser());
			return request.getRemoteUser();
		}

		if (request.getUserPrincipal() != null) {
			log.info(REMOTEMESSAGE + request.getRequestedSessionId() + " as "
					+ request.getRemoteUser() + " using principal " + request.getUserPrincipal());
			return request.getUserPrincipal().getName();
		}
		String authorization = request.getHeader("Authorization");

		log.info(REMOTEMESSAGE + request.getRequestedSessionId()
				+ "after fetching failed using principal.");
		if (authorization != null && authorization.startsWith("Basic")) {
			String base64Credentials = authorization.substring("Basic".length()).trim();
			String credentials = new String(Base64.getDecoder().decode(base64Credentials), Charset.forName("UTF-8"));
			final String[] values = credentials.split(":", 2);
			log.info(REMOTEMESSAGE + request.getRequestedSessionId() + " as "
					+ values[0] + "after decoding.");
			return values[0];
		}else {
			String[] variables = { "Authorization" };

			DataSetRestError err = DataSetErrorList.buildError(ErrorListEnum.E_1001, variables, null,
					CmlpApplicationEnum.DATASOURCE);

			throw new DataSetException(
					"Unauthorized-Missing authorization parameter. Please send all the required information.",
					Status.UNAUTHORIZED.getStatusCode(), err);
		}
	}

	
	public static String getEnv(String envKey, String defaultValue) {
		String value = System.getenv(envKey);
		log.info("getEnv(), environment variable value for envkey " + envKey + "has been searched.");
		if (value == null) {
			log.info("getEnv(), setting value to environment variable value for envkey: " + envKey);
			value = System.getProperty(envKey);
		}
		if (value == null) {
			log.info("getEnv(), setting value to the default value passed for envkey: " + envKey);
			value = defaultValue;
		}
		return value;
	}

	public static String getAPIVersion(HttpServletRequest request) {
		if (apiVersion == null) {
			String requestURI = request.getRequestURI();
			String cntxtPath = request.getContextPath();
			String resourceURI = requestURI.substring(cntxtPath.length() + 1);
			apiVersion = resourceURI.substring(0, resourceURI.indexOf('/'));
		}
		return apiVersion;
	}

	public static String getAPIVersion() {
		return apiVersion;
	}

	public static String getResourceURL(HttpServletRequest request) {
		try {
			if (resourceURL == null) {
				String requestURL = request.getRequestURL().toString();
				String requestURI = request.getRequestURI().toString();

				String baseURL = requestURL.substring(0, requestURL.indexOf(requestURI));
				String ingressPath = HelperTool.getEnv("ingress_service_path",
						HelperTool.getComponentPropertyValue("ingress_service_path"));

				StringBuilder sb = new StringBuilder();
				sb.append(baseURL).append("/").append(ingressPath).append(requestURI);

				resourceURL = sb.toString();

				log.info("requestURL: " + requestURL);
				log.info("requestURI: " + requestURI);
				log.info("baseURL: " + baseURL);
				log.info("ingress_path: " + ingressPath);
				log.info("ResourceURL: " + resourceURL);
			}
		} catch (Exception e) {
			log.error("Error: ", e);
			return request.getRequestURL().toString();
		}

		return resourceURL;
	}
}
