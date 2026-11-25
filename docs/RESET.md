# Reset AWS test data

`scripts/reset_aws_data.py` deletes data while keeping the DynamoDB table, its index and the S3 bucket. Defaults:

- Account: `053109836747`
- Region: `sa-east-1`
- Table: `inpe-focos-clusters`
- Bucket: `inpe-focos-italan-2026-bucket`

It removes **all table items**, including observation/deduplication markers, and **all S3 object versions and delete markers**, including unversioned/null versions. This is permanent. It does not delete resources, purge SQS, change Lambda triggers or remove downloaded local datasets. Existing backups/PITR and incomplete multipart uploads are not erased.

Stop the producer and wait until SQS has zero available/in-flight messages and the consumer has finished before executing. Otherwise messages can repopulate the table during cleanup. Do not redrive the DLQ during a reset.

## CloudShell

Upload `scripts/reset_aws_data.py` with Actions → Upload file. Use your administrative console identity; do not use the restricted producer identity.

```bash
# Preview targets/counts without deleting:
python3 ~/reset_aws_data.py

# Delete all data in the two configured resources:
python3 ~/reset_aws_data.py --execute
```

If boto3 is unavailable, install `python3 -m pip install --user boto3`. The script checks the account and table key schema before deletion and validates S3 ownership. It reports an error and exits nonzero on partial deletion failures; earlier deletions are not rolled back. After fixing a permissions/retention error, rerun.

## Local machine

```bash
source .venv/bin/activate
python scripts/reset_aws_data.py --profile YOUR-CLEANUP-PROFILE --execute
```

Use a profile with cleanup permissions, not `inpe-producer`. The profile needs the following policy for these resources (the script's STS identity call does not require adding an Allow statement):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["dynamodb:DescribeTable", "dynamodb:Scan", "dynamodb:BatchWriteItem"],
      "Resource": "arn:aws:dynamodb:sa-east-1:053109836747:table/inpe-focos-clusters"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:ListBucketVersions"],
      "Resource": "arn:aws:s3:::inpe-focos-italan-2026-bucket"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:DeleteObject", "s3:DeleteObjectVersion"],
      "Resource": "arn:aws:s3:::inpe-focos-italan-2026-bucket/*"
    }
  ]
}
```

## Replay local records

An AWS reset does not reset the producer's local send checkpoints. To also clear them, run the script **locally** while the producer is stopped:

```bash
python scripts/reset_aws_data.py --profile YOUR-CLEANUP-PROFILE \
  --execute --reset-checkpoints
```

It backs up SQLite to `data/state.sqlite3.before-reset.bak`, clears only `sent` entries for the project's queue, and preserves downloaded CSVs and consolidated observations. It refuses to overwrite an existing backup. Override `--state-file` and `--queue-url` if needed. For a CloudShell reset, the local checkpoints must still be cleared on your own machine.

Before resending identical messages, allow at least five minutes since their previous sends because of SQS FIFO deduplication. Restart using your producer profile. S3 snapshots are uploaded again and DynamoDB markers/aggregates are rebuilt from replayed observations.

## Full local reset (downloads, consolidated records and checkpoints)

Stop the producer, then run from the project root:

```bash
python3 scripts/clear_send_checkpoints.py --full-reset
```

This deletes `data/raw/`, `data/consolidated.csv`, temporary consolidated files, `data/state.sqlite3`, its SQLite sidecars and `.before-reset.bak` backups. The lock file remains so that concurrent producer processes use the same lock inode. Other unrelated files are preserved. No AWS calls are made and credentials are unchanged. `DATA_DIR` or `--state-file /path/to/data/state.sqlite3` selects another producer data directory.

The next producer run downloads the currently available source files and rebuilds the dataset from scratch. Local files no longer available from INPE cannot be recovered by redownloading. If AWS has not also been cleared, the consumer's DynamoDB markers will continue deduplicating existing observations. SQS FIFO's five-minute deduplication window still applies.
