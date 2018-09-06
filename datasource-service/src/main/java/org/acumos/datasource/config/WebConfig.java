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

package org.acumos.datasource.config;

import java.io.IOException;
import java.util.Arrays;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;


@Configuration
public class WebConfig {

	@Value("${datasrc_mongo_username}")
	private String datasrc_mongo_username;
	
	@Value("${datasrc_mongo_dbname}")
	private String datasrc_mongo_dbname;
	
	@Value("${datasrc_mongo_password}")
	private String datasrc_mongo_password;
	
	@Value("${datasrc_mongo_hostname}")
	private String datasrc_mongo_hostname;
	
	@Value("${datasrc_mongo_port}")
	private String datasrc_mongo_port;
	
	@Value("${datasrc_mongo_collection_name}")
	private String datasrc_mongo_collection_name;
	
	@Bean
	public DBCollection getDBCollection() throws IOException {
		MongoClient mongoClient = null;
		MongoCredential mongoCredential = MongoCredential.createCredential(
				datasrc_mongo_username,
				datasrc_mongo_dbname,
				datasrc_mongo_password.toCharArray());

		ServerAddress server = new ServerAddress(
				datasrc_mongo_hostname,
				Integer.parseInt(datasrc_mongo_port));
		mongoClient = new MongoClient(server, Arrays.asList(mongoCredential));

		DB datasourceDB = mongoClient.getDB(datasrc_mongo_dbname);

		return datasourceDB.getCollection(datasrc_mongo_collection_name);
	}

	@Bean
	public DBCollection getDBCollection(String username, String password, String dbName, String hostName, String portNumber, String collectionName) throws IOException {
		MongoClient mongoClient = null;
		MongoCredential mongoCredential = MongoCredential.createCredential(
				username,
				dbName,
				password.toCharArray());

		ServerAddress server = new ServerAddress(
				hostName,
				Integer.parseInt(portNumber));
		mongoClient = new MongoClient(server, Arrays.asList(mongoCredential));

		return mongoClient.getDB(dbName).getCollection(collectionName);
	}
}
