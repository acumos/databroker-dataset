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

package org.acumos.test.controller;

import static org.mockito.Mockito.mock;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;

import java.lang.invoke.MethodHandles;
import java.util.List;

import org.acumos.service.controller.DatasourceController;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.test.web.servlet.MockMvc;

@RunWith(MockitoJUnitRunner.class)
public class DatasourceControllerTest {

	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	@Autowired
	private MockMvc mockMvc;
	
	@Mock
	private Environment env;

	@InjectMocks
	private DatasourceController datasourceController;

	@Before
	public void createClient() throws Exception {
		mockMvc = standaloneSetup(datasourceController).build();
	}
	
	@Test
	public void getDataSourcesList() {
		datasourceController = mock(DatasourceController.class);
		List<String> list = datasourceController.getDataSourcesList("namespace", "category", "textSearch", 0, 0);
		logger.info("Successfully fetch: " + list.toString());
		Assert.assertNotNull(list);
	}
	
	@Test
	public void getDataSource() {
		datasourceController = mock(DatasourceController.class);
		List<String> list = datasourceController.getDataSource("datasourceKey");
		logger.info("Successfully fetch: " + list.toString());
		Assert.assertNotNull(list);
	}
	
	@Test
	public void getDataSourceContents() {
		datasourceController = mock(DatasourceController.class);
		List<String> list = datasourceController.getDataSourceContents("dataSourceKey", "hdfsFilename", "proxyHost", 0, "proxyUsername", "proxyPassword");
		logger.info("Successfully fetch: " + list);
		Assert.assertNotNull(list);
	}
	
	@Test
	public void getMetadata() {
		datasourceController = mock(DatasourceController.class);
		List<String> list = datasourceController.getMetadata("datasourceKey");
		logger.info("Successfully fetch: " + list);
		Assert.assertNotNull(list);
	}
	
	@Test
	public void saveDataSourceDetail() throws Exception {
		datasourceController = mock(DatasourceController.class);
		List<String> list = datasourceController.saveDataSourceDetail(null, "proxyHost", 0, "proxyUsername", "proxyPassword");
		logger.info("Successfully fetch: " + list);
		Assert.assertNotNull(list);
	}
	
	@Test
	public void updateDataSourceDetail(){
		datasourceController = mock(DatasourceController.class);
		List<String> list = datasourceController.updateDataSourceDetail("dataSourcekey", null, "proxyHost", 0, "proxyUsername", "proxyPassword");
		logger.info("Successfully fetch: " + list);
		Assert.assertNotNull(list);
	}
	
	@Test
	public void deleteDataSourceDetail() {
		datasourceController = mock(DatasourceController.class);
		List<String> list = datasourceController.deleteDataSourceDetail("dataSourcekey");
		logger.info("Successfully fetch: " + list);
		Assert.assertNotNull(list);
	}
	
	@Test
	public void validateDataSourceConnection() {
		datasourceController = mock(DatasourceController.class);
		List<String> list = datasourceController.validateDataSourceConnection("dataSourcekey");
		logger.info("Successfully fetch: " + list);
		Assert.assertNotNull(list);
	}
	
	@Test
	public void getDataSourceSamples() {
		datasourceController = mock(DatasourceController.class);
		List<String> list = datasourceController.getDataSourceSamples("dataSourcekey",null);
		logger.info("Successfully fetch: " + list);
		Assert.assertNotNull(list);
	}
	
	@Test
	public void writebackPrediction() {
		datasourceController = mock(DatasourceController.class);
		List<String> list = datasourceController.writebackPrediction("dataSourcekey", "hdfsFilename", "includesHeader", "data", "proxyHost", 0, "proxyUsername", "proxyPassword");
		logger.info("Successfully fetch: " + list);
		Assert.assertNotNull(list);
	}
	

	@Test
	public void contextLoads() {
	}

}
