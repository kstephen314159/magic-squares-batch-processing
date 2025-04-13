import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
// import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as firehose from 'aws-cdk-lib/aws-kinesisfirehose';

export class MagicSquare4Stack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // The code that defines your stack goes here

    // example resource
    // const queue = new sqs.Queue(this, 'IacQueue', {
    //   visibilityTimeout: cdk.Duration.seconds(300)
    // });

    const m4Bucket = new s3.Bucket(this, 'm4.squares.megadodo.umb', {
      bucketName: "m4.squares.megadodo.umb",
      autoDeleteObjects: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY
    });
    new firehose.DeliveryStream(this, 'magic-square-stream', {
      deliveryStreamName: "magic-square-stream",
      destination: new firehose.S3Bucket(m4Bucket)
    });
  }
}
