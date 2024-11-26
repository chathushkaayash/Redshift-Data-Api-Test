
package org.example;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.redshift.RedshiftClient;

/**
 * The module containing all dependencies required by the {@link Handler}.
 */
public class DependencyFactory {

    private DependencyFactory() {}

    /**
     * @return an instance of RedshiftClient
     */
    public static RedshiftClient redshiftClient() {
        return RedshiftClient.builder()
                .region(Region.US_EAST_1)
                       .httpClientBuilder(ApacheHttpClient.builder())
                        .credentialsProvider(ProfileCredentialsProvider.create("Admin-Access-367134611783"))
                       .build();
    }
}
