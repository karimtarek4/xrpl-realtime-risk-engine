"""Test DynamoDB connection and verify the account_features table exists.

Reads credentials from .env (never committed to git).
Run via: make test-dynamodb
"""
import os
import sys
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

load_dotenv()

TABLE_NAME = "features"


def get_client():
    return boto3.resource(
        "dynamodb",
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


def test_connection(dynamodb):
    """List tables to verify credentials and connectivity."""
    client = dynamodb.meta.client
    response = client.list_tables()
    tables = response.get("TableNames", [])
    print(f"✓ Connected to DynamoDB — tables found: {tables or '(none yet)'}")
    return tables


def test_table(dynamodb, tables):
    """Check if account_features table exists, create it if not."""
    if TABLE_NAME in tables:
        table = dynamodb.Table(TABLE_NAME)
        table.load()
        print(f"✓ Table '{TABLE_NAME}' exists — status: {table.table_status}")
        return

    print(f"  Table '{TABLE_NAME}' not found — creating...")
    table = dynamodb.create_table(
        TableName=TABLE_NAME,
        KeySchema=[
            {"AttributeName": "account_id", "KeyType": "HASH"},   # partition key
            {"AttributeName": "feature_set", "KeyType": "RANGE"},  # sort key: "rt" | "batch"
        ],
        AttributeDefinitions=[
            {"AttributeName": "account_id", "AttributeType": "S"},
            {"AttributeName": "feature_set", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",  # no capacity planning needed for dev/demo
        # TTL is enabled separately via update_time_to_live after creation
    )
    table.wait_until_exists()

    # Enable TTL on expires_at so DynamoDB auto-deletes stale features
    dynamodb.meta.client.update_time_to_live(
        TableName=TABLE_NAME,
        TimeToLiveSpecification={"Enabled": True, "AttributeName": "expires_at"},
    )
    print(f"✓ Table '{TABLE_NAME}' created with TTL on 'expires_at'")


def main():
    print(f"\nTesting DynamoDB connection...")
    try:
        dynamodb = get_client()
        tables = test_connection(dynamodb)
        test_table(dynamodb, tables)
        print("\n✓ All checks passed\n")
    except NoCredentialsError:
        print("✗ No credentials found — check your .env file", file=sys.stderr)
        sys.exit(1)
    except ClientError as e:
        print(f"✗ AWS error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
