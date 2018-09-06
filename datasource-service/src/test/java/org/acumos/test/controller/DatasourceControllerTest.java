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
import org.acumos.service.schema.DataSourceMetadata;
import org.acumos.service.schema.DataSourceModel;
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
		List<DataSourceModel> list = datasourceController.getDataSourcesList("namespace", "category", "textSearch", 0, 0);
		Assert.assertNotNull(list);
	}
	
	@Test
	public void getDataSource() {
		datasourceController = mock(DatasourceController.class);
		DataSourceModel list = datasourceController.getDataSource("datasourceKey");
		Assert.assertNull(list);
	}
	
	@Test
	public void getDataSourceContents() {
		datasourceController = mock(DatasourceController.class);
		String list = datasourceController.getDataSourceContents("dataSourceKey", "hdfsFilename", "proxyHost", 0, "proxyUsername", "proxyPassword");
		Assert.assertNull(list);
	}
	
	@Test
	public void getMetadata() {
		datasourceController = mock(DatasourceController.class);
		List<DataSourceMetadata> list = datasourceController.getMetadata("datasourceKey");
		Assert.assertNotNull(list);
	}
	
	@Test
	public void saveDataSourceDetail() throws Exception {
		datasourceController = mock(DatasourceController.class);
		DataSourceModel list = datasourceController.saveDataSourceDetail(null, "proxyHost", 0, "proxyUsername", "proxyPassword");
		Assert.assertNull(list);
	}
	
	@Test
	public void updateDataSourceDetail(){
		datasourceController = mock(DatasourceController.class);
		DataSourceModel list = datasourceController.updateDataSourceDetail("dataSourcekey", null, "proxyHost", 0, "proxyUsername", "proxyPassword");
		Assert.assertNull(list);
	}
	
	@Test
	public void deleteDataSourceDetail() {
		datasourceController = mock(DatasourceController.class);
		DataSourceModel list = datasourceController.deleteDataSourceDetail("dataSourcekey");
		Assert.assertNull(list);
	}
	
	@Test
	public void validateDataSourceConnection() {
		datasourceController = mock(DatasourceController.class);
		DataSourceModel list = datasourceController.validateDataSourceConnection("dataSourcekey");
		Assert.assertNull(list);
	}
	
	@Test
	public void getDataSourceSamples() {
		datasourceController = mock(DatasourceController.class);
		String list = datasourceController.getDataSourceSamples("dataSourcekey",null);
		Assert.assertNull(list);
	}
	
	@Test
	public void writebackPrediction() {
		datasourceController = mock(DatasourceController.class);
		DataSourceModel list = datasourceController.writebackPrediction("dataSourcekey", "hdfsFilename", "includesHeader", "data", "proxyHost", 0, "proxyUsername", "proxyPassword");
		Assert.assertNull(list);
	}
	

	@Test
	public void contextLoads() {
	}

}
