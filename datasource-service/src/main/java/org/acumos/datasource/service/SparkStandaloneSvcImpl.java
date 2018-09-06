package org.acumos.datasource.service;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;

import org.slf4j.LoggerFactory;
import org.acumos.datasource.common.HelperTool;
import org.acumos.datasource.exception.CmlpDataSrcException;
import org.acumos.datasource.model.SparkSAModel;
import org.slf4j.Logger;

@Service
public class SparkStandaloneSvcImpl implements SparkStandaloneSvc {

	private static Logger log = LoggerFactory.getLogger(SparkStandaloneSvcImpl.class);

	public SparkStandaloneSvcImpl() {
	}

	@Override
	public String getConnectionStatus(String sparkHostName, String port) throws IOException, InterruptedException, CmlpDataSrcException {
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
	public String getConnectionStatus(SparkSAModel sparkObject) throws IOException, InterruptedException, CmlpDataSrcException {
		
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
