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

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;

import org.slf4j.LoggerFactory;
import org.acumos.datasource.common.HelperTool;
import org.acumos.datasource.exception.DataSrcException;
import org.acumos.datasource.model.SparkSAModel;
import org.slf4j.Logger;

@Service
public class SparkStandaloneSvcImpl implements SparkStandaloneSvc {

	private static Logger log = LoggerFactory.getLogger(SparkStandaloneSvcImpl.class);

	public SparkStandaloneSvcImpl() {
	}

	@Override
	public String getConnectionStatus(String sparkHostName, String port) throws IOException, InterruptedException, DataSrcException {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster("spark://" + sparkHostName + ":" + port).setAppName("sprkTest")
				.set("spark.submit.deployMode",
						HelperTool.getEnv("spark_deploy_mode",
								HelperTool.getComponentPropertyValue("spark_deploy_mode")))
				.set("spark.driver.memory",
						HelperTool.getEnv("spark_driver_memory",
								HelperTool.getComponentPropertyValue("spark_driver_memory")))
				.set("spark.executor.memory",
						HelperTool.getEnv("spark_executor_memory",
								HelperTool.getComponentPropertyValue("spark_executor_memory")))
				.setSparkHome(System.getProperty("user.dir") + System.getProperty("file.separator")
						+ "spark-2.1.1-bin-hadoop2.6")
				.setJars(new String[] { HelperTool.getEnv("spark_test_jar_http",
						HelperTool.getComponentPropertyValue("spark_test_jar_http")) });

		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		
		try {
			if (jsc.sc().applicationId() != null) {
				log.info("The generated application id is " + jsc.sc().applicationId());
				jsc.stop();
				return "success";
			}
		} catch (Exception e) { 
			log.info(e.getMessage());
		} finally {
			if(jsc != null)
				jsc.close();
		}
		
		return "failed";
	}

	@Override
	public String getConnectionStatus(SparkSAModel sparkObject) throws IOException, InterruptedException, DataSrcException {
		
		if (sparkObject.getSparkHostName() != null 
				&& sparkObject.getPort() != null 
				&& sparkObject.getSparkAppName() != null
				&& sparkObject.getSparkDriverMemory() != null
				&& sparkObject.getSparkExecutorMemory() != null
				&& sparkObject.getFatJarLocation() != null){
			SparkConf sparkConf = new SparkConf();
			sparkConf.setMaster("spark://" + sparkObject.getSparkHostName() + ":" + sparkObject.getPort()).setAppName(sparkObject.getSparkAppName())
					.set("spark.submit.deployMode",
							HelperTool.getEnv("spark_deploy_mode",
									HelperTool.getComponentPropertyValue("spark_deploy_mode")))
					.set("spark.driver.memory",
							sparkObject.getSparkDriverMemory())
					.set("spark.executor.memory",
							sparkObject.getSparkExecutorMemory())
					.setSparkHome(System.getProperty("user.dir") + System.getProperty("file.separator")
							+ "spark-2.1.1-bin-hadoop2.6")
					.setJars(new String[] { sparkObject.getFatJarLocation() });

			JavaSparkContext jsc = new JavaSparkContext(sparkConf);
			try {
				if (jsc.sc().applicationId() != null) {
					log.info("The generated application id is " + jsc.sc().applicationId());
					jsc.stop();
					return "success";
				}
			} catch (Exception e) { 
				log.info(e.getMessage());
			} finally {
				if(jsc != null)
					jsc.close();
			}
			
		}
		
		return "failed";
		
	}


}
