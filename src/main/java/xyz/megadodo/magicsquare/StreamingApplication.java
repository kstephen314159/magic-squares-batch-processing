package xyz.megadodo.magicsquare;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.QueryExecutionContext;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.ResultConfiguration;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionResponse;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Datapoint;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.DimensionFilter;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricStatisticsRequest;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricStatisticsResponse;
import software.amazon.awssdk.services.cloudwatch.model.ListMetricsRequest;
import software.amazon.awssdk.services.cloudwatch.model.ListMetricsResponse;
import software.amazon.awssdk.services.cloudwatch.model.Metric;
import software.amazon.awssdk.services.cloudwatch.model.Statistic;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchRequest;
import software.amazon.awssdk.services.firehose.model.PutRecordRequest;
import software.amazon.awssdk.services.firehose.model.Record;




public class StreamingApplication {

    private static FirehoseClient firehoseClient;
    private static CloudWatchClient cloudWatchClient;
    private static CloudWatchAsyncClient cloudWatchAsyncClient;
    
    private static CloudWatchAsyncClient getAsyncClient() {
        if (cloudWatchAsyncClient == null) {
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

            cloudWatchAsyncClient = CloudWatchAsyncClient.builder()
                .httpClient(httpClient)
                .overrideConfiguration(overrideConfig)
                .build();
        }
        return cloudWatchAsyncClient;
    }
    
    public static void main(String[] args) 
    {
        final ArrayList<List<Integer>> batch = new ArrayList<List<Integer>>();
        final Partition pObject = new Partition(4);
        final int[] counter = new int[]{0};

        pObject.getPartionsForSquare().forEach(p -> {
            batch.add(p);
            if(batch.size() == 10){
                    List<Record> partitionBatch = batch.stream().map(record -> {
                        return Record.builder().data(SdkBytes.fromString("{\"row\": " + record.toString() + ", \"repValue\": " + pObject.getRepresentativeValue(record) + "}\n", StandardCharsets.UTF_8)).build();
                    }).toList();
                    PutRecordBatchRequest batchRequest = PutRecordBatchRequest.builder().deliveryStreamName("magic-square-stream").records(partitionBatch).build();
                    getFirehoseClient().putRecordBatch(batchRequest);
                    counter[0]++;
                    batch.clear();
            };
        });
        if(!batch.isEmpty()){
            List<Record> partitionBatch = batch.stream().map(record -> {
                return Record.builder().data(SdkBytes.fromString("{\"row\": " + record.toString() + ", \"repValue\": " + pObject.getRepresentativeValue(record) + "}\n", StandardCharsets.UTF_8)).build();
            }).toList();
            PutRecordBatchRequest batchRequest = PutRecordBatchRequest.builder().deliveryStreamName("magic-square-stream").records(partitionBatch).build();
            getFirehoseClient().putRecordBatch(batchRequest);
            counter[0]++;
            batch.clear();
        }

        CloudWatchClient cwc = CloudWatchClient.create();
        String nextToken = null;
        boolean done = false;

            while(!done){
            ListMetricsResponse response;
            if (nextToken == null) {
                ListMetricsRequest request = ListMetricsRequest.builder()
                    .namespace("AWS/Firehose")
                    .dimensions(DimensionFilter.builder().name("DeliveryStreamName").value("magic-square-stream").build())
                    .build();

                response = cwc.listMetrics(request);
            } else {
                ListMetricsRequest request = ListMetricsRequest.builder()
                    .namespace("AWS/Firehose")
                    .dimensions(DimensionFilter.builder().name("DeliveryStreamName").value("magic-square-stream").build())
                    .nextToken(nextToken)
                    .build();

               response = cwc.listMetrics(request);
            }
            GetMetricStatisticsRequest metricStatisticsRequest = GetMetricStatisticsRequest.builder()
                .startTime(Instant.now().minus(Duration.ofMinutes(1)))
                .endTime(Instant.now().plus(Duration.ofMinutes(5)))
                .dimensions(Dimension.builder().name("DeliveryStreamName").value("magic-square-stream").build())
                .metricName("DeliveryToS3.Records")
                .namespace("AWS/Firehose")
                .period(1)
                .statistics(Statistic.fromValue("SampleCount"))
                .build();
            for(int i = 0; i < 5; i++){
                CompletableFuture<GetMetricStatisticsResponse> future = getAsyncClient().getMetricStatistics(metricStatisticsRequest)
                    .whenComplete((metricsResponse, exception) -> {
                        if (metricsResponse != null) {
                            List<Datapoint> data = metricsResponse.datapoints();
                            if (!data.isEmpty()) {
                                for (Datapoint datapoint : data) {
                                    System.out.println("Timestamp: " + datapoint.timestamp() + " Maximum value: " + datapoint.maximum() + " - " + datapoint.sum() + " - " + datapoint.sampleCount() + " - " + datapoint.hasExtendedStatistics());
                                    datapoint.sdkFields().forEach(field -> {
                                        System.out.println("field = " + field.memberName());
                                    });
                                }
                            } else {
                                System.out.println("The returned data list is empty");
                            }
                        } else {
                            System.out.println("Failed to get metric statistics:" +  exception.getMessage());
                        }
                    })
                    .exceptionally(exception -> {
                        throw new RuntimeException("Error while getting metric statistics: " + exception.getMessage(), exception);
                    });
                future.join();
                try {
                    Thread.sleep(60000);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
    // for (Metric metric : response.metrics()) {
    //             System.out.printf(
    //                     "Retrieved metric %s - %s\n", metric.metricName(), metric.toString());
    //             metric.sdkFields().forEach((f) -> {
    //                 System.out.println(f);
    //             });
    //         }

            if(response.nextToken() == null) {
                done = true;
            } else {
                nextToken = response.nextToken();
            }
        }

        // QueryExecutionContext queryExecutionContext = QueryExecutionContext.builder().database("magic_squares").build();
        // try {
        //     String content = Files.readString(Path.of("src/main/sql/level2.sql"));
        //     ResultConfiguration resultConfiguration = ResultConfiguration.builder().outputLocation("s3://stash.megadodo.umb/magic-square-query-results/").build();
        //     StartQueryExecutionRequest startQueryExecutionRequest = StartQueryExecutionRequest.builder()
        //         .queryString(content).queryExecutionContext(queryExecutionContext).resultConfiguration(resultConfiguration).build();
        //     AthenaClient athenaClient = AthenaClient.builder().build();
        //     StartQueryExecutionResponse startQueryExecutionResponse = athenaClient.startQueryExecution(startQueryExecutionRequest);
        //     String queryExecutionId = startQueryExecutionResponse.queryExecutionId();
        //     waitForQueryToComplete(athenaClient, queryExecutionId);

        //     content = Files.readString(Path.of("src/main/sql/level3.sql"));
        //     startQueryExecutionRequest = StartQueryExecutionRequest.builder()
        //         .queryString(content).queryExecutionContext(queryExecutionContext).resultConfiguration(resultConfiguration).build();
        //     startQueryExecutionResponse = athenaClient.startQueryExecution(startQueryExecutionRequest);
        //     queryExecutionId = startQueryExecutionResponse.queryExecutionId();
        //     waitForQueryToComplete(athenaClient, queryExecutionId);

        //     content = Files.readString(Path.of("src/main/sql/candidate-rowsets.sql"));
        //     startQueryExecutionRequest = StartQueryExecutionRequest.builder()
        //         .queryString(content).queryExecutionContext(queryExecutionContext).resultConfiguration(resultConfiguration).build();
        //     startQueryExecutionResponse = athenaClient.startQueryExecution(startQueryExecutionRequest);
        //     queryExecutionId = startQueryExecutionResponse.queryExecutionId();
        //     waitForQueryToComplete(athenaClient, queryExecutionId);

        // } catch (IOException e) {
        //     // TODO Auto-generated catch block
        //     e.printStackTrace();
        // } catch (InterruptedException e) {
        //     // TODO Auto-generated catch block
        //     e.printStackTrace();
        // }
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
