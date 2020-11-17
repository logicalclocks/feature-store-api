
If you select a S3 bucket then HopsFS will store all the files in the S3 bucket. Do not forget to set
appropriate instance profile so that the cluster instances can access the selected bucket.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/iam_role.png">
      <img src="../../../assets/images/hopsworksai/aws/iam_role.png" alt="Configuring Instance Profile for Hopsworks cluster">
    </a>
    <figcaption>Configuring Instance Profile for Hopsworks cluster</figcaption>
  </figure>
</p>

Following is an example of an instance profile needed by HopsFS to store the file system blocks in a S3 bucket.

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
                  "arn:aws:s3:::bucket.name/*",
                  "arn:aws:s3:::bucket.name"
              ]
          }
      ]
  }
```

Replace *bucket.name* with appropriate S3 bucket name.
