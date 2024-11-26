package org.example;

import software.amazon.awssdk.services.redshift.RedshiftClient;


public class Handler {
    private final RedshiftClient redshiftClient;

    public Handler() {
        redshiftClient = DependencyFactory.redshiftClient();
    }

    public void sendRequest() {
        // TODO: invoking the api calls using redshiftClient.
    }
}
