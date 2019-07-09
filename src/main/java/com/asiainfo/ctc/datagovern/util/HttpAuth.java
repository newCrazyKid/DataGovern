package com.asiainfo.ctc.datagovern.util;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;

import java.util.Base64;

public class HttpAuth {

    private static String authStr = "admin:admin";
    private static String base64authStr = Base64.getEncoder().encodeToString(authStr.getBytes());
    private static HttpHeaders httpHeaders = new HttpHeaders();
    private static HttpEntity<String> entity = null;

    public static HttpEntity<String> getHttpEntity() {
        if(entity == null) {
            synchronized (HttpAuth.class) {
                if(entity == null) {
                    httpHeaders.add("Authorization", "Basic " + base64authStr);
                    entity = new HttpEntity<>(httpHeaders);
                }
            }
        }
        return entity;
    }
}
