package org.example;


public class App {

    public static void main(String... args) {
        String databaseName = "dev";
        String clusterId = "redshift-cluster-integration";
        String userName = "awsuser";

        RedshiftDataClientWrapper redshiftDataClient = new RedshiftDataClientWrapper(clusterId, databaseName, userName);

        long time = System.currentTimeMillis();

        redshiftDataClient.query("SELECT firstname FROM Users limit 10");
//        redshiftDataClient.queryAsync("SELECT firstname FROM Users limit 10");

        System.out.println("Time taken: " + (System.currentTimeMillis() - time));

    }
}
