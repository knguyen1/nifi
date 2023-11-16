/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.salesforce.rest;

import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.ssl.SSLContextService;

import okhttp3.Credentials;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.Proxy;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;

public class SalesforceRestClient {

    private static final MediaType JSON_MEDIA_TYPE = MediaType.parse("application/json; charset=utf-8");

    private final SalesforceConfiguration configuration;
    private final OkHttpClient httpClient;

    /**
     * Constructs a new SalesforceRestClient using the provided SalesforceConfiguration
     * and defaults for ProxyConfiguration, SSLContextService, and HttpProtocolStrategy
     * as null. This constructor is suitable for simple cases where proxy configuration,
     * SSL context service, and HTTP protocol strategy are not required.
     *
     * @param configuration The configuration settings for Salesforce. This must not be null.
     */
    public SalesforceRestClient(SalesforceConfiguration configuration) {
        this(configuration, null, null, null);
    }

    /**
     * Constructs a new SalesforceRestClient using the provided configurations. 
     * This constructor allows for greater control and should be used when the client
     * requires specific proxy configuration, SSL context, or HTTP protocol strategy.
     *
     * @param configuration         The configuration settings for Salesforce. This must not be null.
     * @param proxyConfiguration    The proxy configuration. This can be null if no proxy is used.
     * @param sslContext            The SSL context service. This can be null if SSL context is not required.
     * @param httpProtocolStrategy  The HTTP protocol strategy. This can be null if a specific strategy is not required.
     */
    public SalesforceRestClient(final SalesforceConfiguration configuration,
        final ProxyConfiguration proxyConfiguration,
        final SSLContextService sslContext,
        final HttpProtocolStrategy httpProtocolStrategy) {
            this.configuration = configuration;
            this.httpClient = initializeHttpClient(proxyConfiguration, sslContext, httpProtocolStrategy);
    }

    /**
     * Initializes an OkHttpClient instance using the provided ProxyConfiguration, SSLContextService,
     * and HttpProtocolStrategy. The method configures the OkHttpClient instance with read timeout,
     * SSL socket factory, proxy, and HTTP protocols.
     *
     * If the SSLContextService, ProxyConfiguration, or HttpProtocolStrategy instances are null, this method
     * will skip the respective configurations.
     *
     * @param proxyConfiguration  The proxy configuration. Can be null.
     * @param sslService          The SSL context service. Can be null.
     * @param httpProtocolStrategy The HTTP protocol strategy. Can be null.
     * @return Returns a fully configured OkHttpClient instance.
     */
    private OkHttpClient initializeHttpClient(final ProxyConfiguration proxyConfiguration,
        final SSLContextService sslService,
        final HttpProtocolStrategy httpProtocolStrategy) {
        final OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder();

        // set the default timeout
        clientBuilder.readTimeout(configuration.getResponseTimeoutMillis(), TimeUnit.MILLISECONDS);

        // pass the ssl service to client builder if provided
        if (sslService != null) {
            final X509TrustManager trustManager = sslService.createTrustManager();
            final SSLContext sslContext = sslService.createContext();
            clientBuilder.sslSocketFactory(sslContext.getSocketFactory(), trustManager);
        }

        if (proxyConfiguration != null) {
            final Proxy proxy = proxyConfiguration.createProxy();
            if (!Proxy.Type.DIRECT.equals(proxy.type())) {
                clientBuilder.proxy(proxy);

                if (proxyConfiguration.hasCredential()) {
                    clientBuilder.proxyAuthenticator((route, response) -> {
                        final String credential = Credentials.basic(proxyConfiguration.getProxyUserName(), proxyConfiguration.getProxyUserPassword());
                        return response.request().newBuilder()
                            .header("Proxy-Authorization", credential)
                            .build();
                    });
                }
            }
        }

        if (httpProtocolStrategy != null) clientBuilder.protocols(httpProtocolStrategy.getProtocols());

        return clientBuilder.build();
    }

    public InputStream describeSObject(String sObject) {
        String url = getUrl("/sobjects/" + sObject + "/describe?maxRecords=1");
        Request request = buildGetRequest(url);
        return executeRequest(request);
    }

    public InputStream query(String query) {
        HttpUrl httpUrl = HttpUrl.get(getUrl("/query")).newBuilder()
                .addQueryParameter("q", query)
                .build();
        Request request = buildGetRequest(httpUrl.toString());
        return executeRequest(request);
    }

    public InputStream getNextRecords(String nextRecordsUrl) {
        HttpUrl httpUrl = HttpUrl.get(configuration.getInstanceUrl() + nextRecordsUrl).newBuilder().build();
        Request request = buildGetRequest(httpUrl.toString());
        return executeRequest(request);
    }

    public void postRecord(String sObjectApiName, String body) {
        HttpUrl httpUrl = HttpUrl.get(getUrl("/composite/tree/" + sObjectApiName)).newBuilder().build();
        RequestBody requestBody = RequestBody.create(body, JSON_MEDIA_TYPE);
        Request request = buildPostRequest(httpUrl.toString(), requestBody);
        executeRequest(request);
    }

    private InputStream executeRequest(Request request) {
        Response response = null;
        try {
            response = httpClient.newCall(request).execute();
            if (!response.isSuccessful()) {
                throw new ProcessException(String.format("Invalid response [%s]: %s", response.code(), response.body() == null ? null : response.body().string()));
            }
            return response.body().byteStream();
        } catch (IOException e) {
            if (response != null) {
                response.close();
            }
            throw new UncheckedIOException(String.format("Salesforce HTTP request failed [%s]", request.url()), e);
        }
    }

    private String getUrl(String path) {
        return getVersionedBaseUrl() + path;
    }

    public String getVersionedBaseUrl() {
        return configuration.getInstanceUrl() + "/services/data/v" + configuration.getVersion();
    }

    private Request buildGetRequest(String url) {
        return new Request.Builder()
                .addHeader("Authorization", "Bearer " + configuration.getAccessTokenProvider().get())
                .url(url)
                .get()
                .build();
    }

    private Request buildPostRequest(String url, RequestBody requestBody) {
        return new Request.Builder()
                .addHeader("Authorization", "Bearer " + configuration.getAccessTokenProvider().get())
                .url(url)
                .post(requestBody)
                .build();
    }
}
