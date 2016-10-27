package com.microsoft.azure.documentdb;

import java.util.Arrays;
import java.util.List;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.microsoft.azure.documentdb.directconnectivity.HttpClientFactory;

@RunWith(Parameterized.class)
public abstract class ParameterizedGatewayTestBase extends GatewayTestBase {
    public ParameterizedGatewayTestBase(DocumentClient client) {
        super(client);
    }

    @Parameters
    public static List<Object[]> configs() {
        // To run direct connectivity tests we need to disable host name verification because replicas host is not localhost
        HttpClientFactory.DISABLE_HOST_NAME_VERIFICATION = true;

        DocumentClient gatewayClient = new DocumentClient(HOST, MASTER_KEY, ConnectionPolicy.GetDefault(), ConsistencyLevel.Session);

        ConnectionPolicy policy = new ConnectionPolicy();
        policy.setConnectionMode(ConnectionMode.DirectHttps);
        DocumentClient directHttpClient = new DocumentClient(HOST, MASTER_KEY, policy, ConsistencyLevel.Session);
        return Arrays.asList(new Object[][] {
                {directHttpClient},
                {gatewayClient}
        });
    }
}

