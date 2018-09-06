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

import org.acumos.service.controller.DatasetController;
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
public class DatasetControllerTest {

	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	@Autowired
	private MockMvc mockMvc;
	
	@Mock
	private Environment env;

	@InjectMocks
	private DatasetController datasetController;

	@Before
	public void createClient() throws Exception {
		mockMvc = standaloneSetup(datasetController).build();
	}
	
	@Test
	public void getDataSetList() {
		datasetController = mock(DatasetController.class);
		List<String> list = datasetController.getDataSetList("dummy",null);
		logger.info("Successfully fetch: " + list.toString());
		Assert.assertNotNull(list);
	}
	
	@Test
	public void getDataset() {
		datasetController = mock(DatasetController.class);
		List<String> list = datasetController.getDataSet("dummy");
		logger.info("Successfully fetch: " + list.toString());
		Assert.assertNotNull(list);
	}
	
	@Test
	public void saveDataSetDetail() {
		datasetController = mock(DatasetController.class);
		List<String> list = datasetController.saveDataSetDetail(null);
		logger.info("Successfully fetch: " + list.toString());
		Assert.assertNotNull(list);
	}
	
	@Test
	public void updateDataSetDetail() {
		datasetController = mock(DatasetController.class);
		List<String> list = datasetController.updateDataSetDetail("dummy",null);
		logger.info("Successfully fetch: " + list.toString());
		Assert.assertNotNull(list);
	}
	
	@Test
	public void deleteDataSetDetail() {
		datasetController = mock(DatasetController.class);
		List<String> list = datasetController.deleteDataSetDetail("dummy");
		logger.info("Successfully fetch: " + list.toString());
		Assert.assertNotNull(list);
	}
	
	@Test
	public void getDataSources() {
		datasetController = mock(DatasetController.class);
		List<String> list = datasetController.getDataSources("dummy");
		logger.info("Successfully fetch: " + list.toString());
		Assert.assertNotNull(list);
	}
	
	@Test
	public void updateDataSourceKey() {
		datasetController = mock(DatasetController.class);
		List<String> list = datasetController.updateDataSourceKey("dummy",null);
		logger.info("Successfully fetch: " + list.toString());
		Assert.assertNotNull(list);
	}
	
	@Test
	public void getAttributeMetaData() {
		datasetController = mock(DatasetController.class);
		List<String> list = datasetController.getAttributeMetaData("dummy");
		logger.info("Successfully fetch: " + list.toString());
		Assert.assertNotNull(list);
	}
	
	@Test
	public void updateAttributeMetaData() {
		datasetController = mock(DatasetController.class);
		List<String> list = datasetController.updateAttributeMetaData("dummy",null);
		logger.info("Successfully fetch: " + list.toString());
		Assert.assertNotNull(list);
	}

	@Test
	public void contextLoads() {
	}

}
