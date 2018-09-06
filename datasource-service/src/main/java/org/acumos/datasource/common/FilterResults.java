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

package org.acumos.datasource.common;

import java.util.ArrayList;
import java.util.HashMap;

import javax.ws.rs.core.Link;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

public class FilterResults {
	
	public static ArrayList<String> returnDatasets(ArrayList<String> dbResults, int pgOffset, int pgLimit) throws Exception {

		//if no offset and limit values, return the complete set
		if(pgOffset == 0 && pgLimit == 0)
			return dbResults;
		
		//check for the input
		if(dbResults == null || dbResults.size() == 0)
			return dbResults;
		
		int startIndex = (pgOffset - 1) * pgLimit;
				
		if(dbResults.size() > startIndex ) {
			
			int endIndex = ((startIndex + pgLimit) > dbResults.size()) ? dbResults.size() : startIndex + pgLimit;
			
			return new ArrayList<String>(dbResults.subList(startIndex, endIndex));
			
		} else if(dbResults.size() == startIndex || startIndex > dbResults.size()) {
			return new ArrayList<String>();
		} 
		
		return dbResults;
	}

	public static Response setPaginationRecord(int page, int perPage, int totalRecords, String baseURL, ResponseBuilder httpResponse) {
		
		HashMap<String, String> linkRecords = new HashMap<String, String>();
		
		int total = 0;
		if(totalRecords > 0) {
			if((totalRecords % perPage) == 0 )
				total = totalRecords/perPage;
			else
				total = totalRecords/perPage + 1;
		}
		
		StringBuffer sb;
		
		if(page != 1) {
			sb = new StringBuffer();
			sb.append(baseURL).append("?page=").append(1).append("&perPage=").append(perPage);
			linkRecords.put("first", sb.toString());
		}
		
		if(page <= total) {
			sb = new StringBuffer();
			sb.append(baseURL).append("?page=").append(page).append("&perPage=").append(perPage);
			linkRecords.put("self", sb.toString());
			
			int nextPage = page + 1;
			
			if(nextPage < total) {
				sb = new StringBuffer();
				sb.append(baseURL).append("?page=").append(nextPage).append("&perPage=").append(perPage);
				linkRecords.put("next", sb.toString());
			}
			
			int previousPage = page - 1;
			
			if(previousPage != 0) {
				sb = new StringBuffer();
				sb.append(baseURL).append("?page=").append(previousPage).append("&perPage=").append(perPage);
				linkRecords.put("previous", sb.toString());
			}
		}
		
		if(page != total) {
			sb = new StringBuffer();
			sb.append(baseURL).append("?page=").append(total).append("&perPage=").append(perPage);
			linkRecords.put("last", sb.toString());
		}
		
		Link[] links = new Link[linkRecords.size()];
		
		int i = 0;
		
		if(linkRecords.containsKey("first")) {
			links[i++] = Link.fromUri(linkRecords.get("first")).rel("first").build();
			
		}
		if(linkRecords.containsKey("previous")) {
			links[i++] = Link.fromUri(linkRecords.get("previous")).rel("previous").build();
		}
		
		if(linkRecords.containsKey("self")) {
			links[i++] = Link.fromUri(linkRecords.get("self")).rel("self").build();
		}
		
		if(linkRecords.containsKey("next")) {
			links[i++] = Link.fromUri(linkRecords.get("next")).rel("next").build();
		}
		
		if(linkRecords.containsKey("last")) {
			links[i++] = Link.fromUri(linkRecords.get("last")).rel("last").build();
		}
		
		Response response = httpResponse.build();
		
		sb = new StringBuffer();
		for(int j=0; j<links.length; j++) {
			if(j > 0) {
				sb.append(", ");
			}
			sb.append(links[j].toString());
		}
		
		response.getHeaders().add("Link", sb.toString());
		
		return response;
	}
}
