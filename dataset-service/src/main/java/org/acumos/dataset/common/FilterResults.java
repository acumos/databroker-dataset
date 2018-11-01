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

package org.acumos.dataset.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.ws.rs.core.Link;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

public class FilterResults {

	private static final String PAGE = "?page=";
	private static final String PERPAGE = "&perPage=";
	private static final String FIRST = "first";
	private static final String PREVIOUS = "previous";
	private static final String SELF = "self";
	private static final String LAST = "last";
	private static final String NEXT = "next";
	
	private FilterResults() {
		super();
	}
	
	public static List<String> returnDatasets(List<String> dbResults, int pgOffset, int pgLimit){
		// if no offset and limit values, return the complete set
		if (pgOffset == 0 && pgLimit == 0)
			return dbResults;

		if (dbResults == null || dbResults.isEmpty())
			return dbResults;

		int startIndex = (pgOffset - 1) * pgLimit;

		if (dbResults.size() > startIndex) {

			int endIndex = ((startIndex + pgLimit) > dbResults.size()) ? dbResults.size() : startIndex + pgLimit;

			return new ArrayList<>(dbResults.subList(startIndex, endIndex));

		} else if (dbResults.size() == startIndex || startIndex > dbResults.size()) {
			return new ArrayList<>();
		}

		return dbResults;
	}

	public static Response setPaginationRecord(int page, int perPage, int totalRecords, String baseURL,
			ResponseBuilder httpResponse) {

		HashMap<String, String> linkRecords = new HashMap<>();

		int total = 0;
		if (totalRecords > 0) {
			if ((totalRecords % perPage) == 0)
				total = totalRecords / perPage;
			else
				total = totalRecords / perPage + 1;
		}

		StringBuilder sb;

		if (page != 1) {
			sb = new StringBuilder();
			sb.append(baseURL).append(PAGE).append(1).append(PERPAGE).append(perPage);
			linkRecords.put(FIRST, sb.toString());
		}

		if (page <= total) {
			setLinkRecords(page, perPage, total, baseURL, linkRecords);
		}

		if (page != total) {
			sb = new StringBuilder();
			sb.append(baseURL).append(PAGE).append(total).append(PERPAGE).append(perPage);
			linkRecords.put(LAST, sb.toString());
		}

		Link[] links = new Link[linkRecords.size()];
		setLink(linkRecords, links);

		Response response = httpResponse.build();

		sb = new StringBuilder();
		for (int j = 0; j < links.length; j++) {
			if (j > 0) {
				sb.append(", ");
			}
			sb.append(links[j].toString());
		}

		response.getHeaders().add("link", sb.toString());

		return response;
	}

	private static void setLink(HashMap<String, String> linkRecords, Link[] links) {
		int i = 0;
		if (linkRecords.containsKey(FIRST)) {
			links[i++] = Link.fromUri(linkRecords.get(FIRST)).rel(FIRST).build();

		}
		if (linkRecords.containsKey(PREVIOUS)) {
			links[i++] = Link.fromUri(linkRecords.get(PREVIOUS)).rel(PREVIOUS).build();
		}

		if (linkRecords.containsKey(SELF)) {
			links[i++] = Link.fromUri(linkRecords.get(SELF)).rel(SELF).build();
		}

		if (linkRecords.containsKey(NEXT)) {
			links[i++] = Link.fromUri(linkRecords.get(NEXT)).rel(NEXT).build();
		}

		if (linkRecords.containsKey(LAST)) {
			links[i] = Link.fromUri(linkRecords.get(LAST)).rel(LAST).build();
		}
	}

	private static void setLinkRecords(int page, int perPage, int total, String baseURL,
			HashMap<String, String> linkRecords) {
		StringBuilder sb = new StringBuilder();
		sb.append(baseURL).append(PAGE).append(page).append(PERPAGE).append(perPage);
		linkRecords.put(SELF, sb.toString());

		int nextPage = page + 1;

		if (nextPage < total) {
			sb = new StringBuilder();
			sb.append(baseURL).append(PAGE).append(nextPage).append(PERPAGE).append(perPage);
			linkRecords.put(NEXT, sb.toString());
		}

		int previousPage = page - 1;

		if (previousPage != 0) {
			sb = new StringBuilder();
			sb.append(baseURL).append(PAGE).append(previousPage).append(PERPAGE).append(perPage);
			linkRecords.put(PREVIOUS, sb.toString());
		}
	}

}
