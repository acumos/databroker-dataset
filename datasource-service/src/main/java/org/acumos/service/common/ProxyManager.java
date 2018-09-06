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

import java.net.Authenticator;

/**
 * Proxy Manager
 * 
 * This class provides a mechanism for configuring proxies for HTTP, HTTPS and FTP protocols.
 * 
 * Please note that a major limitation of using system properties for configuring proxies is that they are an “all or nothing” switch. 
 * 
 * Meaning that once a proxy has been set for a particular protocol, it will affect all connections for that protocol. 
 * 
 * It's a VM wide behavior.
 * 
 * After the connect is completed, the proxy properties should be removed or reset.
 * 
 * More details are available here:
 * 
 * https://docs.oracle.com/javase/8/docs/technotes/guides/net/proxies.html
 * https://docs.oracle.com/javase/8/docs/api/java/net/Authenticator.html
 * 
 */
public class ProxyManager {
	
	private static final String PROXY_HTTP_HOST = "http.proxyHost";
	private static final String PROXY_HTTP_PORT = "http.proxyPort";
	
	private static final String PROXY_HTTPS_HOST = "https.proxyHost";
	private static final String PROXY_HTTPS_PORT = "https.proxyPort";
	
	private static final String PROXY_FTP_HOST = "ftp.proxHost";
	private static final String PROXY_FTP_PORT = "ftp.proxPort";
	
	private static final String STRING_PADDING = "";
	
	public static final String PROXY_HOST_DEFAULT = "0";
	public static final String PROXY_PORT_DEFAULT = "0";
	public static final String PROXY_USER_DEFAULT = "0";
	public static final String PROXY_PASS_DEFAULT = "0";
	
	
	public static void setUpProxy(String host, int port, String username, String password) {
		
		setUpProxy(host, port + STRING_PADDING);
		
		if (username == null || password == null ) {
			return;
		}
		
		if (username.equals(PROXY_USER_DEFAULT) || password.equals(PROXY_PASS_DEFAULT)) {
			return;
		}
		
		Authenticator.setDefault(new ProxyAuthenticator(username, password));
	}
	
	
	public static void setUpProxy(String host, int port) {
		
		setUpProxy(host, port + STRING_PADDING);
	}

	
	private static void setUpProxy(String host, String port) {
		
		if (host == null || port == null) {
			
			return;
		}
		
		if (host.equals(PROXY_HOST_DEFAULT) || port.equals(PROXY_PORT_DEFAULT))  {

			return;
		}
			
		System.setProperty(PROXY_HTTP_HOST, host);
        System.setProperty(PROXY_HTTP_PORT, port);
        
        System.setProperty(PROXY_HTTPS_HOST, host);
        System.setProperty(PROXY_HTTPS_PORT, port);
        
        System.setProperty(PROXY_FTP_HOST, host);
        System.setProperty(PROXY_FTP_PORT, port);
		
	}
	
	
	public static void tearDownProxy() {
		
		System.clearProperty(PROXY_HTTP_HOST);
		System.clearProperty(PROXY_HTTP_PORT);
		
		System.clearProperty(PROXY_HTTPS_HOST);
		System.clearProperty(PROXY_HTTPS_PORT);
		
		System.clearProperty(PROXY_FTP_HOST);
		System.clearProperty(PROXY_FTP_PORT);
	}

}

