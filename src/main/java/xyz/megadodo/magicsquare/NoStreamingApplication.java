package xyz.megadodo.magicsquare;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.gson.Gson;

import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketInfo;
import software.amazon.awssdk.services.s3.model.BucketType;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;

public class NoStreamingApplication {

    public static void main(String[] args) {

        S3Client s3 = S3Client.builder().forcePathStyle(true).build();
        CreateBucketConfiguration bucketConfiguration = CreateBucketConfiguration.builder().bucket(BucketInfo.builder().type(BucketType.DIRECTORY).build()).build();
        CreateBucketRequest req = CreateBucketRequest.builder().bucket("xyz.megadodo.magicsquare.partition").createBucketConfiguration(bucketConfiguration).build();
        CreateBucketResponse resp = s3.createBucket(req);
        System.out.println(resp.location());

        PutObjectRequest putReq = PutObjectRequest.builder().bucket("xyz.megadodo.magicsquare.partition").key("all-partitions.json").build();
        try {
            File tempFile = File.createTempFile("all-partitions", "json");
            BufferedWriter bw = new BufferedWriter(new FileWriter(tempFile));
            final Partition pObject = new Partition();
            AtomicInteger counter = new AtomicInteger();
            pObject.getPartionsForSquare(65, 5).forEach(p -> {
                try {
                    bw.append("{\"id\": " + counter.incrementAndGet() + ", \"partition\": " + p.toString() + ", \"repValue\": " + pObject.getRepresentativeValue(p) + "}\n");
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            });
            bw.close();
            s3.putObject(putReq, RequestBody.fromFile(tempFile));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // System.out.println((new Partition()).getPartionsForSquare(65, 5).size());
    }
}
