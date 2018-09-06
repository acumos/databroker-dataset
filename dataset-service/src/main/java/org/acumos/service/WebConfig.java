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

package org.acumos.service;

import org.springframework.context.annotation.Configuration;

/**
 * 
 * @author am375y
 *
 */
@Configuration
public class WebConfig {

	/*
	private static final String USERNAME = "datasrc_mongo_username";
	private static final String DBNAME = "datasrc_mongo_dbname";
	private static final String PD = "datasrc_mongo_password";
	private static final String HOSTNAME = "datasrc_mongo_hostname";
	private static final String PORT = "datasrc_mongo_port";
	private static final String COLLECTION_NAME = "dataset_mongo_collection_name";

	@Bean
	public DBCollection getDBCollection() throws IOException {
		MongoClient mongoClient = null;
		MongoCredential mongoCredential = MongoCredential.createCredential(
				HelperTool.getEnv(USERNAME, HelperTool.getComponentPropertyValue(USERNAME)),
				HelperTool.getEnv(DBNAME, HelperTool.getComponentPropertyValue(DBNAME)),
				HelperTool.getEnv(PD, HelperTool.getComponentPropertyValue(PD)).toCharArray());

		ServerAddress server = new ServerAddress(
				HelperTool.getEnv(HOSTNAME, HelperTool.getComponentPropertyValue(HOSTNAME)),
				Integer.parseInt(HelperTool.getEnv(PORT, HelperTool.getComponentPropertyValue(PORT))));
		mongoClient = new MongoClient(server, Arrays.asList(mongoCredential));

		DB datasetDB = mongoClient.getDB(HelperTool.getEnv(DBNAME, HelperTool.getComponentPropertyValue(DBNAME)));

		return datasetDB.getCollection(
				HelperTool.getEnv(COLLECTION_NAME, HelperTool.getComponentPropertyValue(COLLECTION_NAME)));
	}

	@Bean
	public DB getDatasetDB() throws IOException {
		MongoClient mongoClient = null;

		MongoCredential mongoCredential = MongoCredential.createCredential(
				HelperTool.getEnv(USERNAME, HelperTool.getComponentPropertyValue(USERNAME)),
				HelperTool.getEnv(DBNAME, HelperTool.getComponentPropertyValue(DBNAME)),
				HelperTool.getEnv(PD, HelperTool.getComponentPropertyValue(PD)).toCharArray());

		ServerAddress server = new ServerAddress(
				HelperTool.getEnv(HOSTNAME, HelperTool.getComponentPropertyValue(HOSTNAME)),
				Integer.parseInt(HelperTool.getEnv(PORT, HelperTool.getComponentPropertyValue(PORT))));
		mongoClient = new MongoClient(server, Arrays.asList(mongoCredential));

		return mongoClient.getDB(HelperTool.getEnv(DBNAME, HelperTool.getComponentPropertyValue(DBNAME)));
	}
	*/
}
