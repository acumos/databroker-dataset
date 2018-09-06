package org.acumos.service.test.controller;

import static org.mockito.Mockito.mock;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;

import java.lang.invoke.MethodHandles;

import javax.ws.rs.core.Response;

import org.acumos.dataset.controller.DatasetController;
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
	public void getDataSourcesList() {
		datasetController = mock(DatasetController.class);
		Response res = datasetController.getDataSetList("authorization", "textSearch", null, 0, 10);
		Assert.assertNull(res);
	}
	
	@Test
	public void getDataSet() {
		datasetController = mock(DatasetController.class);
		Response res = datasetController.getDataSet("authorization", "test");
		Assert.assertNull(res);
	}
	
	@Test
	public void saveDataSetDetail() {
		datasetController = mock(DatasetController.class);
		Response res = datasetController.saveDataSetDetail("authorization", null);
		Assert.assertNull(res);
	}
	
	@Test
	public void updateDataSetDetail() {
		datasetController = mock(DatasetController.class);
		Response res = datasetController.updateDataSetDetail("authorization", "dataSetKey", null);
		Assert.assertNull(res);
	}
	
	@Test
	public void deleteDataSetDetail() {
		datasetController = mock(DatasetController.class);
		Response res = datasetController.deleteDataSetDetail("authorization", "test");
		Assert.assertNull(res);
	}
	
	@Test
	public void getDataSources() {
		datasetController = mock(DatasetController.class);
		Response res = datasetController.getDataSources("authorization", "test");
		Assert.assertNull(res);
	}
	
	@Test
	public void updateDataSourceKey() {
		datasetController = mock(DatasetController.class);
		Response res = datasetController.updateDataSourceKey("authorization", "dataSetKey", null);
		Assert.assertNull(res);
	}
	
	@Test
	public void getAttributeMetaData() {
		datasetController = mock(DatasetController.class);
		Response res = datasetController.getAttributeMetaData("authorization", "test");
		Assert.assertNull(res);
	}
	
	@Test
	public void updateAttributeMetaData() {
		datasetController = mock(DatasetController.class);
		Response res = datasetController.updateAttributeMetaData("authorization", "dataSetKey", null);
		Assert.assertNull(res);
	}
	
}
