
Replace *BUCKET_NAME* with appropriate S3 bucket name.
```json
  {
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "HopsFSS3Permissions",
      "Effect": "Allow",
      "Action": [
        "S3:PutObject",
        "S3:ListBucket",
        "S3:GetBucketLocation",
        "S3:GetObject",
        "S3:DeleteObject",
        "S3:AbortMultipartUpload",
        "S3:ListBucketMultipartUploads"
      ],
      "Resource": [
        "arn:aws:s3:::BUCKET_NAME/*",
        "arn:aws:s3:::BUCKET_NAME"
      ]
    }
  ]
}
```
