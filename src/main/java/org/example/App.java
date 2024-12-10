package org.example;

import org.openjdk.jmh.annotations.*;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.redshiftdata.RedshiftDataAsyncClient;
import software.amazon.awssdk.services.redshiftdata.RedshiftDataClient;
import software.amazon.awssdk.services.redshiftdata.model.*;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import java.util.List;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 2, time = 2, timeUnit = TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class App {

    @Benchmark()
    public static void syncClient() throws InterruptedException {

        String databaseName = "dev";
        String clusterId = "redshift-cluster-integration";
        String userName = "awsuser";
        String query = "SELECT * FROM Users";

        RedshiftDataClientWrapper redshiftDataClient = new RedshiftDataClientWrapper(clusterId, databaseName, userName);

        for (int i = 0; i < 10; i++) {
            String id = redshiftDataClient.executeStatement(query);
            System.out.println("The statement id is: " + id);
        }
    }

    @Benchmark()
    public static void asyncClient() throws InterruptedException {

        String databaseName = "dev";
        String clusterId = "redshift-cluster-integration";
        String userName = "awsuser";
        String query = "SELECT * FROM Users";

        RedshiftDataClientWrapper redshiftDataClient = new RedshiftDataClientWrapper(clusterId, databaseName, userName);

        for (int i = 0; i < 10; i++) {
            String id = redshiftDataClient.asyncExecuteStatement(query).join();
            System.out.println("The statement id is: " + id);
        }

    }

    static class RedshiftDataClientWrapper {
        private static RedshiftDataClient redshiftDataClient;
        private static RedshiftDataAsyncClient redshiftDataAsyncClient;
        private final String clusterId;
        private final String databaseName;
        private final String dbUser;

        static String AWS_ACCESS_KEY_ID = System.getenv("AWS_ACCESS_KEY_ID");
        static String AWS_SECRET_ACCESS_KEY = System.getenv("AWS_SECRET_ACCESS_KEY");

        public RedshiftDataClientWrapper(String clusterId, String databaseName, String dbUser) {
            this.clusterId = clusterId;
            this.databaseName = databaseName;
            this.dbUser = dbUser;
        }

        private static RedshiftDataClient getDataClient() {
            if (redshiftDataClient == null) {
                AwsBasicCredentials credentials = AwsBasicCredentials.builder()
                        .accessKeyId(AWS_ACCESS_KEY_ID)
                        .secretAccessKey(AWS_SECRET_ACCESS_KEY)
                        .build();

                ClientOverrideConfiguration overrideConfig = ClientOverrideConfiguration.builder()
                        .apiCallTimeout(Duration.ofMinutes(2))
                        .apiCallAttemptTimeout(Duration.ofSeconds(90))
                        .retryStrategy(RetryMode.STANDARD)
                        .build();

                redshiftDataClient = RedshiftDataClient.builder()
                        .region(Region.US_EAST_2)
                        .credentialsProvider(StaticCredentialsProvider.create(credentials))
                        .overrideConfiguration(overrideConfig)
                        .build();
            }
            return redshiftDataClient;
        }


        public String executeStatement(String sqlStatement) {

            ExecuteStatementRequest statementRequest = ExecuteStatementRequest.builder()
                    .clusterIdentifier(clusterId)
                    .database(databaseName)
                    .dbUser(dbUser)
                    .sql(sqlStatement)
                    .build();

            ExecuteStatementResponse response = getDataClient().executeStatement(statementRequest); // Use join() to wait for the result
            return response.id();
        }

        private int describeStatement(String statementId) {
            while (true) {
                DescribeStatementRequest describeRequest = DescribeStatementRequest.builder().id(statementId).build();
                DescribeStatementResponse describeResponse = getDataClient().describeStatement(describeRequest);

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
            return 1;
        }


        private GetStatementResultResponse getStatementResult(String statementId) {
            // Prepare the request to get the statement result
            GetStatementResultRequest resultRequest = GetStatementResultRequest.builder().id(statementId).build();
            GetStatementResultResponse resultResponse = getDataClient().getStatementResult(resultRequest);

            // Get column metadata and rows
            if (resultResponse.columnMetadata() == null || resultResponse.records() == null) {
                throw new RuntimeException("No results found for query.");
            }

            System.out.println("The result is: " + resultResponse.records());
            return resultResponse;

        }


        //---------------------- Async Functions ---------------------------------------

        private static RedshiftDataAsyncClient getAsyncDataClient() {
            if (redshiftDataAsyncClient == null) {

                AwsBasicCredentials credentials = AwsBasicCredentials.builder()
                        .accessKeyId(AWS_ACCESS_KEY_ID)
                        .secretAccessKey(AWS_SECRET_ACCESS_KEY)
                        .build();

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
                        .credentialsProvider(StaticCredentialsProvider.create(credentials))
                        .build();
            }
            return redshiftDataAsyncClient;
        }

        public CompletableFuture<String> asyncExecuteStatement(String sqlStatement) {

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

        public CompletableFuture<Void> asyncDescribeStatement(String sqlId) {
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
                            }).thenCompose(ignore -> asyncDescribeStatement(sqlId)); // Recursively call until status is FINISHED or FAILED
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

        public CompletableFuture<Void> asyncGetStatementResult(String statementId) {
            GetStatementResultRequest resultRequest = GetStatementResultRequest.builder()
                    .id(statementId)
                    .build();

            return getAsyncDataClient().getStatementResult(resultRequest)
                    .handle((response, exception) -> {
                        if (exception != null) {
                            throw new RuntimeException("Error getting statement result: " + exception.getMessage(), exception);
                        }

                        // Extract and print the field values using streams if the response is valid.
                        response.records().stream()
                                .flatMap(List::stream)
                                .map(Field::stringValue)
                                .filter(value -> value != null)
                                .forEach(value -> System.out.println("The Movie title field is " + value));

                        return response;
                    }).thenAccept(response -> {
                        // Optionally add more logic here if needed after handling the response
                    });
        }


        public void close() {
            if (redshiftDataAsyncClient != null) {
                redshiftDataAsyncClient.close();
            }
        }
    }
}
