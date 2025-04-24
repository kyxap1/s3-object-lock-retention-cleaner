# S3 Object Lock Retention Cleaner

A tool to unlock and delete objects with retention settings in AWS S3 buckets.

## Overview

This utility script helps manage S3 objects with retention and legal hold settings by:

1. Removing legal holds from objects
2. Setting retention periods to expire immediately
3. Optionally deleting the objects after unlocking them

The tool is especially useful for managing buckets with a large number of objects that have retention policies, which can be difficult to clean up through the AWS console.

## Important Limitations

**This tool only works with objects protected by GOVERNANCE mode retention.**

Objects in COMPLIANCE mode retention cannot be unlocked by this tool or any other method until their retention period expires, as per AWS design. The only way to delete objects in COMPLIANCE mode before their retention period expires is to delete the AWS account itself.

For more information on S3 Object Lock and the differences between retention modes, see [AWS S3 Object Lock documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock-overview.html#object-lock-retention-modes).

## Requirements

- Python 3.6+
- AWS credentials properly configured
- Required Python packages (specified in requirements.txt):
  ```
  boto3>=1.26.0
  botocore>=1.29.0
  ```

## Installation

```bash
git clone https://github.com/kyxap1/s3-object-lock-retention-cleaner.git
cd s3-object-lock-retention-cleaner
pip install -r requirements.txt
```

One-liner for user mode installation:
```bash
python -m pip install --user -r requirements.txt
```

## Usage

```bash
python wipe-bucket.py <bucket-name> [--dry-run] [--only-unlock] [--verbose]
```

### Parameters

- `bucket-name`: (Required) Name of the S3 bucket containing objects to process
- `--dry-run`: Simulate the operation without making any changes
- `--only-unlock`: Only remove retention settings without deleting objects
- `--verbose`: Enable detailed logging of operations

### Examples

**List all objects that would be affected without making changes:**
```bash
python wipe-bucket.py my-bucket --dry-run --verbose
```

**Unlock retention settings but don't delete objects:**
```bash
python wipe-bucket.py my-bucket --only-unlock
```

**Unlock and delete all objects with retention settings:**
```bash
python wipe-bucket.py my-bucket
```

## How It Works

1. The script exports all object versions from the specified bucket to a CSV file
2. For each object version, it:
   - Removes any legal hold
   - Sets retention to GOVERNANCE mode with immediate expiration
   - Deletes the object (unless the `--only-unlock` flag is used)
3. Operations are performed in parallel using Python's ThreadPoolExecutor

## AWS Permissions Required

The AWS credentials used must have the following permissions:
- `s3:ListBucketVersions`
- `s3:GetObjectLegalHold`
- `s3:PutObjectLegalHold`
- `s3:GetObjectRetention`
- `s3:PutObjectRetention`
- `s3:DeleteObject`
- `s3:BypassGovernanceRetention`

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. 