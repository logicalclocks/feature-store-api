
Replace *BUCKET_NAME* with the appropriate S3 bucket name. Non-enterprise users can remove the policies  *S3:PutLifecycleConfiguration*, *S3:GetLifecycleConfiguration*, *S3:PutBucketVersioning*, *S3:GetBucketVersioning* as these policies are needed for cluster backups and restore operations available only for the enterprise version. 

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "hopsworksaiInstanceProfile",
      "Effect": "Allow",
      "Action": [
        "S3:PutObject",
        "S3:ListBucket",
        "S3:GetBucketLocation",
        "S3:GetObject",
        "S3:DeleteObject",
        "S3:AbortMultipartUpload",
        "S3:ListBucketMultipartUploads",
        "S3:PutLifecycleConfiguration",
        "S3:GetLifecycleConfiguration",
        "S3:PutBucketVersioning",
        "S3:GetBucketVersioning"
      ],
      "Resource": [
        "arn:aws:s3:::BUCKET_NAME/*",
        "arn:aws:s3:::BUCKET_NAME"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:PutMetricData",
        "ec2:DescribeVolumes",
        "ec2:DescribeTags",
        "logs:PutLogEvents",
        "logs:DescribeLogStreams",
        "logs:DescribeLogGroups",
        "logs:CreateLogStream",
        "logs:CreateLogGroup"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "ssm:GetParameter"
      ],
      "Resource": "arn:aws:ssm:*:*:parameter/AmazonCloudWatch-*"
    }
  ]
}
```
