package com.microsoft.azure.documentdb.directconnectivity;


import java.util.concurrent.TimeUnit;

import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;

public class HttpClientFactory {
    // Test hook for testing direct https on localhost
    public static boolean DISABLE_HOST_NAME_VERIFICATION = false;

    public static PoolingHttpClientConnectionManager createConnectionManager(int maxPoolSize, int idleConnectionTimeout) {
        PoolingHttpClientConnectionManager connectionManager = null;

        if (DISABLE_HOST_NAME_VERIFICATION) {
            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(SSLContexts.createDefault(), new NoopHostnameVerifier());

            Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder
                    .<ConnectionSocketFactory>create().register("https", sslsf)
                    .build();

            connectionManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
        } else {
            connectionManager = new PoolingHttpClientConnectionManager();
        }

        connectionManager.setMaxTotal(maxPoolSize);
        connectionManager.setDefaultMaxPerRoute(maxPoolSize);
        connectionManager.closeIdleConnections(idleConnectionTimeout, TimeUnit.SECONDS);

        return connectionManager;
    }

    /**
     * Only one instance is created for the httpClient for optimization.
     * A PoolingClientConnectionManager is used with the Http client
     * to be able to reuse connections and execute requests concurrently.
     * A timeout for closing each connection is set so that connections don't leak.
     * A timeout is set for requests to avoid deadlocks.
     *
     * @param connectionManager       the connection manager
     * @param requestTimeoutSeconds   the timeout in seconds
     * @return                        the created HttpClient
     */
    public static HttpClient createHttpClient(PoolingHttpClientConnectionManager connectionManager, int requestTimeoutSeconds) {
        HttpClientBuilder httpClientBuilder = HttpClients.custom()
                .setConnectionManager(connectionManager)
                .disableCookieManagement()
                .setDefaultSocketConfig(SocketConfig.custom().setSoTimeout(requestTimeoutSeconds * 1000).build())
                .setDefaultRequestConfig(RequestConfig.custom().setConnectionRequestTimeout(requestTimeoutSeconds * 1000).build());

        return httpClientBuilder.build();
    }

    public static HttpClient createHttpClient(int maxPoolSize, int idleConnectionTimeout, int requestTimeoutSeconds) {
        PoolingHttpClientConnectionManager connectionPoolingManager = HttpClientFactory.createConnectionManager(maxPoolSize, idleConnectionTimeout);
        return HttpClientFactory.createHttpClient(connectionPoolingManager, requestTimeoutSeconds);
    }
}
