package xyz.megadodo.magicsquare;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import com.google.gson.Gson;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.QueryExecutionContext;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.ResultConfiguration;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionResponse;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.model.PutRecordRequest;
import software.amazon.awssdk.services.firehose.model.Record;




public class StreamingApplication {

    private static FirehoseClient firehoseClient;

    public static void main(String[] args) 
    {
        // final Partition pObject = new Partition();
        // pObject.getPartionsForSquare(65, 5).forEach(p -> {
        //     Record firehoseRecord = Record.builder().data(SdkBytes.fromString("{\"row\": " + p.toString() + ", \"repValue\": " + pObject.getRepresentativeValue(p) + "}\n", StandardCharsets.UTF_8)).build();        
        //     PutRecordRequest request = PutRecordRequest.builder().deliveryStreamName("partition-delivery-stream").record(firehoseRecord).build();
        //     getFirehoseClient().putRecord(request);
        // });

        QueryExecutionContext queryExecutionContext = QueryExecutionContext.builder().database("magic_squares").build();
        try {
            String content = Files.readString(Path.of("src/main/sql/level2.sql"));
            ResultConfiguration resultConfiguration = ResultConfiguration.builder().outputLocation("s3://stash.megadodo.umb/magic-square-query-results/").build();
            StartQueryExecutionRequest startQueryExecutionRequest = StartQueryExecutionRequest.builder()
                .queryString(content).queryExecutionContext(queryExecutionContext).resultConfiguration(resultConfiguration).build();
            AthenaClient athenaClient = AthenaClient.builder().build();
            StartQueryExecutionResponse startQueryExecutionResponse = athenaClient.startQueryExecution(startQueryExecutionRequest);
            String queryExecutionId = startQueryExecutionResponse.queryExecutionId();
            waitForQueryToComplete(athenaClient, queryExecutionId);

            content = Files.readString(Path.of("src/main/sql/level3.sql"));
            startQueryExecutionRequest = StartQueryExecutionRequest.builder()
                .queryString(content).queryExecutionContext(queryExecutionContext).resultConfiguration(resultConfiguration).build();
            startQueryExecutionResponse = athenaClient.startQueryExecution(startQueryExecutionRequest);
            queryExecutionId = startQueryExecutionResponse.queryExecutionId();
            waitForQueryToComplete(athenaClient, queryExecutionId);

            content = Files.readString(Path.of("src/main/sql/candidate-rowsets.sql"));
            startQueryExecutionRequest = StartQueryExecutionRequest.builder()
                .queryString(content).queryExecutionContext(queryExecutionContext).resultConfiguration(resultConfiguration).build();
            startQueryExecutionResponse = athenaClient.startQueryExecution(startQueryExecutionRequest);
            queryExecutionId = startQueryExecutionResponse.queryExecutionId();
            waitForQueryToComplete(athenaClient, queryExecutionId);

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static void waitForQueryToComplete(AthenaClient athenaClient, String queryExecutionId)
            throws InterruptedException {
        GetQueryExecutionRequest getQueryExecutionRequest = GetQueryExecutionRequest.builder()
                .queryExecutionId(queryExecutionId)
                .build();

        GetQueryExecutionResponse getQueryExecutionResponse;
        boolean isQueryStillRunning = true;
        while (isQueryStillRunning) {
            getQueryExecutionResponse = athenaClient.getQueryExecution(getQueryExecutionRequest);
            String queryState = getQueryExecutionResponse.queryExecution().status().state().toString();
            if (queryState.equals(QueryExecutionState.FAILED.toString())) {
                throw new RuntimeException(
                        "The Amazon Athena query failed to run with error message: " + getQueryExecutionResponse
                                .queryExecution().status().stateChangeReason());
            } else if (queryState.equals(QueryExecutionState.CANCELLED.toString())) {
                throw new RuntimeException("The Amazon Athena query was cancelled.");
            } else if (queryState.equals(QueryExecutionState.SUCCEEDED.toString())) {
                isQueryStillRunning = false;
            } else {
                // Sleep an amount of time before retrying again.
                Thread.sleep(10000);
            }
            System.out.println("The current status is: " + queryState);
        }
    }

    private static FirehoseClient getFirehoseClient() {
        if (firehoseClient == null) {
            firehoseClient = FirehoseClient.create();
        }
        return firehoseClient;
    }
}
