package org.acumos.dataset.common;

import com.fasterxml.jackson.annotation.JsonRawValue;
import com.fasterxml.jackson.annotation.JsonValue;

public class JsonResponse {
	
	private final String value;

	public JsonResponse(String value) {
		this.value = value;
	}

	@JsonValue
	@JsonRawValue
	public String value() {
		return value;
	}
}
