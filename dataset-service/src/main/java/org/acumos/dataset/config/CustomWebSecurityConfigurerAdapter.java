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

package org.acumos.dataset.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.web.authentication.www.BasicAuthenticationEntryPoint;

import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * Spring 4 security requires a CSRF token on POST/PUT/DELETE requests. But this
 * server has no web pages where a CSRF token would be sent, so disable.
 * 
 * Use basic HTTP auth, but exclude the health check from Spring security.
 * 
 * With credit to:
 * http://ryanjbaxter.com/2015/01/06/securing-rest-apis-with-spring-boot/
 */
@Configuration
@EnableSwagger2
@EnableWebSecurity
public class CustomWebSecurityConfigurerAdapter extends WebSecurityConfigurerAdapter {

	private static final String REALM_NAME = "Acumos-Dataset";

	/**
	 * Open access to the documentation.
	 */
	@Override
	public void configure(WebSecurity web) throws Exception {
		web.ignoring().antMatchers("/v2/api-docs", "/swagger-resources/**", "/swagger-ui.html", "/webjars/**");
	}

	/**
	 * Open access to the health and version endpoints.
	 */
	@Override
	protected void configure(HttpSecurity http) throws Exception {
		
		http.csrf().disable()
				.authorizeRequests()
				.antMatchers("/healthcheck").permitAll()
				.antMatchers("/version").permitAll()
				.antMatchers("/v2/api-docs", "/swagger-resources/configuration/ui", "/swagger-resources", "/swagger-resources/configuration/security", "/swagger-ui.html", "/webjars/**").permitAll()
				.antMatchers("/**").authenticated()
				.and().httpBasic().realmName(REALM_NAME).authenticationEntryPoint(getBasicAuthEntryPoint());
	}
	

	@Bean
	public BasicAuthenticationEntryPoint getBasicAuthEntryPoint() {
		BasicAuthenticationEntryPoint baep = new CustomBasicAuthenticationEntryPoint();
		baep.setRealmName(REALM_NAME);
		return baep;
	}

}
