package org.example;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.redshiftdata.RedshiftDataAsyncClient;
import software.amazon.awssdk.services.redshiftdata.RedshiftDataClient;
import software.amazon.awssdk.services.redshiftdata.model.*;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class RedshiftDataClientWrapper {
    private static RedshiftDataClient redshiftDataClient;
    private static RedshiftDataAsyncClient redshiftDataAsyncClient;
    private final String clusterId;
    private final String databaseName;
    private final String dbUser;

    public RedshiftDataClientWrapper(String clusterId, String databaseName, String dbUser) {
        this.clusterId = clusterId;
        this.databaseName = databaseName;
        this.dbUser = dbUser;
    }

    private static RedshiftDataClient getDataClient() {
        if (redshiftDataClient == null) {
            ClientOverrideConfiguration overrideConfig = ClientOverrideConfiguration.builder()
                    .apiCallTimeout(Duration.ofMinutes(2))
                    .apiCallAttemptTimeout(Duration.ofSeconds(90))
                    .retryStrategy(RetryMode.STANDARD)
                    .build();

            redshiftDataClient = RedshiftDataClient.builder()
                    .region(Region.US_EAST_2)
                    .credentialsProvider(ProfileCredentialsProvider.create("redshift"))
                    .overrideConfiguration(overrideConfig)
                    .build();
        }
        return redshiftDataClient;
    }


    public void query(String statement) {
        String id = queryRequest(statement);
//        checkStatement(id);
//        getResults(id);
    }


    public String queryRequest(String sqlStatement) {

        ExecuteStatementRequest statementRequest = ExecuteStatementRequest.builder()
                .clusterIdentifier(clusterId)
                .database(databaseName)
                .dbUser(dbUser)
                .sql(sqlStatement)
                .build();

        ExecuteStatementResponse response = getDataClient().executeStatement(statementRequest); // Use join() to wait for the result
        return response.id();
    }

    private void checkStatement(String statementId) {
        while (true) {
            DescribeStatementRequest describeRequest = DescribeStatementRequest.builder().id(statementId).build();
            DescribeStatementResponse describeResponse = redshiftDataClient.describeStatement(describeRequest);

            String status = describeResponse.statusAsString();
            if ("FINISHED".equals(status)) {
                break;
            } else if ("FAILED".equals(status)) {
                throw new RuntimeException("Query failed: " + describeResponse.error());
            } else {
                System.out.println("Query status: " + status + ". Waiting for completion...");
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Thread interrupted while waiting for query completion.", e);
            }
        }
    }


    private void getResults(String statementId) {
        // Prepare the request to get the statement result
        GetStatementResultRequest resultRequest = GetStatementResultRequest.builder().id(statementId).build();
        GetStatementResultResponse resultResponse = getDataClient().getStatementResult(resultRequest);

        // Get column metadata and rows
        if (resultResponse.columnMetadata() == null || resultResponse.records() == null) {
            throw new RuntimeException("No results found for query.");
        }

    }


    //---------------------- Async Functions ---------------------------------------
    public void queryAsync(String statement) {
        queryRequestAsync(statement);
//        String id = queryRequestAsync(statement).join();
//        checkStatementAsync(id).join();
//        getResultsAsync(id).join();
    }

    private static RedshiftDataAsyncClient getAsyncDataClient() {
        if (redshiftDataAsyncClient == null) {
            SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.builder()
                    .maxConcurrency(100)
                    .connectionTimeout(Duration.ofSeconds(60))
                    .readTimeout(Duration.ofSeconds(60))
                    .writeTimeout(Duration.ofSeconds(60))
                    .build();

            ClientOverrideConfiguration overrideConfig = ClientOverrideConfiguration.builder()
                    .apiCallTimeout(Duration.ofMinutes(2))
                    .apiCallAttemptTimeout(Duration.ofSeconds(90))
                    .retryStrategy(RetryMode.STANDARD)
                    .build();

            redshiftDataAsyncClient = RedshiftDataAsyncClient.builder()
                    .httpClient(httpClient)
                    .overrideConfiguration(overrideConfig)
                    .credentialsProvider(ProfileCredentialsProvider.create("redshift"))
                    .build();
        }
        return redshiftDataAsyncClient;
    }

    public CompletableFuture<String> queryRequestAsync(String sqlStatement) {

        ExecuteStatementRequest statementRequest = ExecuteStatementRequest.builder()
                .clusterIdentifier(clusterId)
                .database(databaseName)
                .dbUser(dbUser)
                .sql(sqlStatement)
                .build();

        return CompletableFuture.supplyAsync(() -> {
            try {
                ExecuteStatementResponse response = getAsyncDataClient().executeStatement(statementRequest).join(); // Use join() to wait for the result
                return response.id();
            } catch (RedshiftDataException e) {
                throw new RuntimeException("Error executing statement: " + e.getMessage(), e);
            }
        }).exceptionally(exception -> {
            return "";
        });
    }

    public CompletableFuture<Void> checkStatementAsync(String sqlId) {
        DescribeStatementRequest statementRequest = DescribeStatementRequest.builder()
                .id(sqlId)
                .build();

        return getAsyncDataClient().describeStatement(statementRequest)
                .thenCompose(response -> {
                    String status = response.statusAsString();

                    if ("FAILED".equals(status)) {
                        throw new RuntimeException("The Query Failed. Ending program");
                    } else if ("FINISHED".equals(status)) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        // Sleep for 1 second and recheck status
                        return CompletableFuture.runAsync(() -> {
                            try {
                                TimeUnit.MILLISECONDS.sleep(100);
                            } catch (InterruptedException e) {
                                throw new RuntimeException("Error during sleep: " + e.getMessage(), e);
                            }
                        }).thenCompose(ignore -> checkStatementAsync(sqlId)); // Recursively call until status is FINISHED or FAILED
                    }
                }).whenComplete((result, exception) -> {
                    if (exception != null) {
                        // Handle exceptions
                        System.out.println("Error checking statement: " + exception.getMessage());
                    } else {
                        System.out.println("Statement is finished");
                    }
                });
    }

    public CompletableFuture<Void> getResultsAsync(String statementId) {
        GetStatementResultRequest resultRequest = GetStatementResultRequest.builder()
                .id(statementId)
                .build();

        return getAsyncDataClient().getStatementResult(resultRequest)
                .handle((response, exception) -> {
                    if (exception != null) {
                        throw new RuntimeException("Error getting statement result: " + exception.getMessage(), exception);
                    }

                    // Extract and print the field values using streams if the response is valid.
//                    response.records().stream()
//                            .flatMap(List::stream)
//                            .map(Field::stringValue)
//                            .filter(value -> value != null)
//                            .forEach(value -> System.out.println("The Movie title field is " + value));

                    return response;
                }).thenAccept(response -> {
                    // Optionally add more logic here if needed after handling the response
                });
    }


}



