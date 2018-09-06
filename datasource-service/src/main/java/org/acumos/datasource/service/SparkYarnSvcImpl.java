package org.acumos.datasource.service;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.springframework.stereotype.Service;

import org.slf4j.LoggerFactory;
import org.acumos.datasource.common.HelperTool;
import org.acumos.datasource.common.Utilities;
import org.acumos.datasource.exception.CmlpDataSrcException;
import org.acumos.datasource.model.JobSubmissionYarn;
import org.acumos.datasource.model.KerberosLogin;
import org.acumos.datasource.model.SparkYarnTestModel;
import org.slf4j.Logger;

@Service
public class SparkYarnSvcImpl implements SparkYarnSvc {

	private static Logger log = LoggerFactory.getLogger(SparkYarnSvcImpl.class);

	public SparkYarnSvcImpl() {

	}

	@Override
	public String getConnectionStatusWithKerberos(JobSubmissionYarn objJobSubmissionYarn) throws IOException, CmlpDataSrcException {
		System.setProperty("SPARK_YARN_MODE", "true");

		log.info("preparing to submit job to spark on YARN");
		ArrayList<String> args = new ArrayList<>();
		args.add("--jar");
		args.add(objJobSubmissionYarn.getFatJarLocation());
		args.add("--class");
		args.add(objJobSubmissionYarn.getMainClassName());
		if (!objJobSubmissionYarn.getAppArguments().isEmpty()) {
			args.add("--arg");
			for (String appArg : objJobSubmissionYarn.getAppArguments()) {
				args.add(appArg);
			}
		}

		Configuration config = HelperTool.getKerberisedConfiguration(objJobSubmissionYarn.getHadoopHostName(),
				objJobSubmissionYarn.getKerberosLoginUser());
		config.set("yarn.resourcemanager.address", objJobSubmissionYarn.getYarnRMAddress());
		log.info("YARN RM address in hadoop configuration: " + config.get("yarn.resourcemanager.address"));
		config.set("yarn.resourcemanager.principal",
				"rm/" + objJobSubmissionYarn.getYarnRMPrincipal() + "@" + objJobSubmissionYarn.getKerberosRealm());
		log.info("YARN RM principal in hadoop configuration: " + config.get("yarn.resourcemanager.principal"));

		SparkConf sparkConf = new SparkConf();
		sparkConf.set("spark.yarn.jars", objJobSubmissionYarn.getSparkYarnJar())
				.set("mapreduce.app-submission.cross-platform", "true").setMaster("yarn")
				.setAppName(objJobSubmissionYarn.getAppName())
				.set("spark.submit.deployMode",
						(objJobSubmissionYarn.getDeployMode() != null) ? objJobSubmissionYarn.getDeployMode()
								: HelperTool.getEnv("spark_deploy_mode",
										HelperTool.getComponentPropertyValue("spark_deploy_mode")))
				.set("spark.authenticate", "true")
				.set("spark.yarn.principal", objJobSubmissionYarn.getKerberosLoginUser())
				.set("spark.yarn.keytab", System.getProperty("user.dir") + System.getProperty("file.separator")
						+ objJobSubmissionYarn.getKerberosLoginUser().substring(0,
								objJobSubmissionYarn.getKerberosLoginUser().indexOf("@"))
						+ "." + "kerberos.keytab")
				.set("spark.driver.memory",
						(objJobSubmissionYarn.getDriverMemory() != null ? objJobSubmissionYarn.getDriverMemory()
								: HelperTool.getEnv("spark_driver_memory",
										HelperTool.getComponentPropertyValue("spark_driver_memory"))))
				.set("spark.executor.memory",
						(objJobSubmissionYarn.getExecutorMemory() != null ? objJobSubmissionYarn.getExecutorMemory()
								: HelperTool.getEnv("spark_executor_memory",
										HelperTool.getComponentPropertyValue("spark_executor_memory"))));

		log.info("spark configuration details: spark yarn jar = " + sparkConf.get("spark.yarn.jars")
				+ "/spark deploy mode = " + sparkConf.get("spark.submit.deployMode") + "/spark yarn principal = "
				+ sparkConf.get("spark.yarn.principal"));

		if (objJobSubmissionYarn.getDeployMode() == null || objJobSubmissionYarn.getDeployMode().equals("cluster")) {
			sparkConf.set("spark.driver.extraJavaOptions", "-Dhdp.version=" + objJobSubmissionYarn.getHdpVersion());
		} else if (objJobSubmissionYarn.getDeployMode().equals("client")) {
			sparkConf.set("spark.yarn.am.extraJavaOptions", "-Dhdp.version=" + objJobSubmissionYarn.getHdpVersion());
		}
		log.info("hadoop version: " + objJobSubmissionYarn.getHdpVersion());

		log.info("intiating client arguments");
		ClientArguments cArgs = new ClientArguments(args.toArray(new String[0]));//, sparkConf);

		log.info("intiating yarn client");
		//Client client = new Client(cArgs, config, sparkConf);
		Client client = new Client(cArgs, sparkConf);

		SparkHadoopUtil.get();
		UserGroupInformation.setConfiguration(config);

		log.info("submitting job to spark on yarn and generating application id");
		ApplicationId objApplicationId = client.submitApplication();

		if (objApplicationId != null) {
			log.info(
					"application id for connectivity est using principal " + objJobSubmissionYarn.getKerberosLoginUser()
							+ " on " + objJobSubmissionYarn.getHadoopHostName() + " is " + objApplicationId);
			return "success";
		}
		return null;
	}

	@Override
	public void createKerberosKeytab(KerberosLogin objKerberosLogin) throws IOException, CmlpDataSrcException {
		log.info("Creating kerberos keytab for principal " + objKerberosLogin.getKerberosLoginUser());
		Utilities.createKerberosKeytab(objKerberosLogin);
		log.info("Created kerberos keytab for principal " + objKerberosLogin.getKerberosLoginUser());

	}

	@Override
	public String getConnectionStatusWithKerberos(SparkYarnTestModel objSparkYarnTestModel) throws IOException, CmlpDataSrcException {
		log.info("Creating kerberos keytab for hostname "
				+ objSparkYarnTestModel.getObjJobSubmissionYarn().getHadoopHostName() + " using principal "
				+ objSparkYarnTestModel.getObjJobSubmissionYarn().getKerberosLoginUser()
				+ "for hive connectivity testing.");
		Utilities.createKerberosKeytab(objSparkYarnTestModel.getObjKerberosLogin());
		log.info("Testing hive connectivity for hostname "
				+ objSparkYarnTestModel.getObjJobSubmissionYarn().getHadoopHostName() + " using principal "
				+ objSparkYarnTestModel.getObjJobSubmissionYarn().getKerberosLoginUser()
				+ " after creating kerberos keytab");
		String result = getConnectionStatusWithKerberos(objSparkYarnTestModel.getObjJobSubmissionYarn());
		return result;
	}

}
