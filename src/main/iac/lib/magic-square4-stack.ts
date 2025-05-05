import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
// import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as firehose from 'aws-cdk-lib/aws-kinesisfirehose';
import * as logs from 'aws-cdk-lib/aws-logs';

export class MagicSquare4Stack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // The code that defines your stack goes here

    // example resource
    // const queue = new sqs.Queue(this, 'IacQueue', {
    //   visibilityTimeout: cdk.Duration.seconds(300)
    // });

    const m4Bucket = new s3.Bucket(this, 'm4.squares.megadodo.umb', {
      autoDeleteObjects: true,
      bucketName: "m4.squares.megadodo.umb",
      lifecycleRules: [
        {
          expiration: cdk.Duration.days(3)
        }
      ],
      removalPolicy: cdk.RemovalPolicy.DESTROY
    });

    const logGroup = new logs.LogGroup(this, 'firehose log group', {
      logGroupName: "umb/megadodo/magic_square/m4",
      removalPolicy: cdk.RemovalPolicy.DESTROY
    });

    const magicSquareDeliveryStream = new firehose.DeliveryStream(this, 'magic-square-stream', {
      deliveryStreamName: "magic-square-stream",
      destination: new firehose.S3Bucket(m4Bucket, {
        bufferingInterval: cdk.Duration.seconds(0),
        bufferingSize: cdk.Size.mebibytes(100),
        loggingConfig: new firehose.EnableLogging(logGroup)
      })
    });

  }
}
