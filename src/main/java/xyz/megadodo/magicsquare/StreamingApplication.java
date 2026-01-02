package xyz.megadodo.magicsquare;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import software.amazon.awssdk.core.SdkBytes;
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
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchRequest;
import software.amazon.awssdk.services.firehose.model.Record;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Object;




public class StreamingApplication {

    private static FirehoseClient firehoseClient;
    private static CloudWatchClient cloudWatchClient;
    private static CloudWatchAsyncClient cloudWatchAsyncClient;
    private static S3Client s3Client;
    
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
        if (args.length == 0) {
            System.err.println("Usage: StreamingApplication <4|5> [sqlStatementNumber]");
            System.exit(1);
        }
        
        String orderArg = args[0];
        if (!orderArg.equals("4") && !orderArg.equals("5")) {
            System.err.println("Argument must be 4 or 5");
            System.exit(1);
        }
        
        int sqlStatementNumber = 0; // Default to start from beginning
        if (args.length > 1) {
            try {
                sqlStatementNumber = Integer.parseInt(args[1]);
                if (sqlStatementNumber < 0) {
                    System.err.println("sqlStatementNumber must be >= 0");
                    System.exit(1);
                }
            } catch (NumberFormatException e) {
                System.err.println("sqlStatementNumber must be a valid integer");
                System.exit(1);
            }
        }
        
        // Only generate and send partitions to Firehose if sqlStatementNumber is 0
        if (sqlStatementNumber == 0) {
            final ArrayList<List<Integer>> batch = new ArrayList<List<Integer>>();
            final Partition pObject = new Partition(Integer.parseInt(orderArg));
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

            System.out.println("Sent " + counter[0] + " batches to Firehose. Waiting 90 seconds for buffer to flush...");
            try {
                Thread.sleep(90000); // Wait for buffering_interval (60s) + safety margin
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Proceeding with Athena queries...");
        } else {
            System.out.println("Skipping Firehose data generation (sqlStatementNumber = " + sqlStatementNumber + ")");
            System.out.println("Proceeding directly to Athena queries...");
        }

        QueryExecutionContext queryExecutionContext = QueryExecutionContext.builder().database("magic_squares").build();
        ResultConfiguration resultConfiguration = ResultConfiguration.builder().outputLocation("s3://m4.squares.megadodo.umb/query-results/").build();
        AthenaClient athenaClient = AthenaClient.builder().build();
        
        try {
            // Read sequence.json from resources
            String sequenceFile = "m" + orderArg + "/sequence.json";
            InputStream sequenceStream = StreamingApplication.class.getClassLoader().getResourceAsStream(sequenceFile);
            if (sequenceStream == null) {
                throw new IOException("Could not find " + sequenceFile + " in resources");
            }
            
            // Parse JSON object with create_statements and delete_statements
            String jsonContent = new String(sequenceStream.readAllBytes(), StandardCharsets.UTF_8);
            SqlSequence sqlSequence = parseSequenceJson(jsonContent);
            
            // Validate sqlStatementNumber
            if (sqlStatementNumber >= sqlSequence.createStatements.size()) {
                System.err.println("sqlStatementNumber (" + sqlStatementNumber + ") is >= number of SQL statements (" + sqlSequence.createStatements.size() + ")");
                System.exit(1);
            }
            
            // Execute each SQL file in sequence, starting from sqlStatementNumber
            for (int i = sqlStatementNumber; i < sqlSequence.createStatements.size(); i++) {
                // If sqlStatementNumber is provided (> 0), run delete statement first, then delete S3 data
                if (sqlStatementNumber > 0) {
                    // Drop the table first
                    String deleteFile = sqlSequence.deleteStatements.get(i);
                    System.out.println("Executing DELETE file [" + i + "]: " + deleteFile);
                    String deleteContent = Files.readString(Path.of(deleteFile));
                    StartQueryExecutionRequest deleteRequest = StartQueryExecutionRequest.builder()
                        .queryString(deleteContent).queryExecutionContext(queryExecutionContext).resultConfiguration(resultConfiguration).build();
                    StartQueryExecutionResponse deleteResponse = athenaClient.startQueryExecution(deleteRequest);
                    waitForQueryToComplete(athenaClient, deleteResponse.queryExecutionId());
                    
                    // Then delete S3 data if path is specified
                    String s3Path = sqlSequence.deletePaths.get(i);
                    if (!s3Path.isEmpty()) {
                        deleteS3Data(s3Path);
                    }
                }
                
                // Run create statement
                String createFile = sqlSequence.createStatements.get(i);
                System.out.println("Executing CREATE file [" + i + "]: " + createFile);
                String createContent = Files.readString(Path.of(createFile));
                StartQueryExecutionRequest createRequest = StartQueryExecutionRequest.builder()
                    .queryString(createContent).queryExecutionContext(queryExecutionContext).resultConfiguration(resultConfiguration).build();
                StartQueryExecutionResponse createResponse = athenaClient.startQueryExecution(createRequest);
                waitForQueryToComplete(athenaClient, createResponse.queryExecutionId());
            }

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
    
    private static S3Client getS3Client() {
        if (s3Client == null) {
            s3Client = S3Client.create();
        }
        return s3Client;
    }
    
    private static void deleteS3Data(String s3Uri) {
        // Parse S3 URI (s3://bucket/prefix/)
        if (!s3Uri.startsWith("s3://")) {
            System.err.println("Invalid S3 URI: " + s3Uri);
            return;
        }
        
        String pathPart = s3Uri.substring(5); // Remove "s3://"
        int slashIndex = pathPart.indexOf('/');
        if (slashIndex == -1) {
            System.err.println("Invalid S3 URI format: " + s3Uri);
            return;
        }
        
        String bucket = pathPart.substring(0, slashIndex);
        String prefix = pathPart.substring(slashIndex + 1);
        
        System.out.println("Deleting S3 data from s3://" + bucket + "/" + prefix);
        
        S3Client s3 = getS3Client();
        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
            .bucket(bucket)
            .prefix(prefix)
            .build();
        
        ListObjectsV2Response listResponse = s3.listObjectsV2(listRequest);
        
        if (listResponse.contents().isEmpty()) {
            System.out.println("No objects to delete");
            return;
        }
        
        List<ObjectIdentifier> objectsToDelete = new ArrayList<>();
        for (S3Object s3Object : listResponse.contents()) {
            objectsToDelete.add(ObjectIdentifier.builder().key(s3Object.key()).build());
        }
        
        Delete delete = Delete.builder().objects(objectsToDelete).build();
        DeleteObjectsRequest deleteRequest = DeleteObjectsRequest.builder()
            .bucket(bucket)
            .delete(delete)
            .build();
        
        s3.deleteObjects(deleteRequest);
        System.out.println("Deleted " + objectsToDelete.size() + " objects from S3");
    }
    
    private static class SqlSequence {
        List<String> createStatements;
        List<String> deleteStatements;
        List<String> deletePaths;
    }
    
    private static SqlSequence parseSequenceJson(String json) {
        // Simple JSON parser for the sequence structure
        SqlSequence result = new SqlSequence();
        result.createStatements = new ArrayList<>();
        result.deleteStatements = new ArrayList<>();
        result.deletePaths = new ArrayList<>();
        
        // Extract create_statements array
        int createStart = json.indexOf("\"create_statements\"");
        int createArrayStart = json.indexOf("[", createStart);
        int createArrayEnd = json.indexOf("]", createArrayStart);
        String createArrayContent = json.substring(createArrayStart + 1, createArrayEnd);
        
        // Extract delete_statements array
        int deleteStart = json.indexOf("\"delete_statements\"");
        int deleteArrayStart = json.indexOf("[", deleteStart);
        int deleteArrayEnd = json.indexOf("]", deleteArrayStart);
        String deleteArrayContent = json.substring(deleteArrayStart + 1, deleteArrayEnd);
        
        // Extract delete_paths array
        int pathsStart = json.indexOf("\"delete_paths\"");
        int pathsArrayStart = json.indexOf("[", pathsStart);
        int pathsArrayEnd = json.indexOf("]", pathsArrayStart);
        String pathsArrayContent = json.substring(pathsArrayStart + 1, pathsArrayEnd);
        
        // Parse create statements
        String[] createParts = createArrayContent.split(",");
        for (String part : createParts) {
            String cleaned = part.trim().replaceAll("\"", "");
            if (!cleaned.isEmpty()) {
                result.createStatements.add(cleaned);
            }
        }
        
        // Parse delete statements
        String[] deleteParts = deleteArrayContent.split(",");
        for (String part : deleteParts) {
            String cleaned = part.trim().replaceAll("\"", "");
            if (!cleaned.isEmpty()) {
                result.deleteStatements.add(cleaned);
            }
        }
        
        // Parse delete paths
        String[] pathsParts = pathsArrayContent.split(",");
        for (String part : pathsParts) {
            String cleaned = part.trim().replaceAll("\"", "");
            // Allow empty strings for paths where no deletion is needed
            result.deletePaths.add(cleaned);
        }
        
        return result;
    }
}
