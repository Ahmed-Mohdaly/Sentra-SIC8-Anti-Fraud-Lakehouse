#!/usr/bin/env python3
"""
Amazon EventBridge Scheduler Management Utility for SIC Data Pipeline.

Enables listing, enabling, disabling, and checking the status of serverless ingestion schedules.
"""

import sys
import argparse
import boto3
from botocore.exceptions import ClientError

REGION = "eu-west-3"
SCHEDULES = [
    "sic-streaming-ingestion-15min",
    "sic-batch-reference-daily"
]

def get_client():
    return boto3.client("scheduler", region_name=REGION)

def status():
    client = get_client()
    print(f"\n{'Schedule Name':<32} | {'State':<10} | {'Expression':<22} | {'Target Task'}")
    print("-" * 90)
    for name in SCHEDULES:
        try:
            s = client.get_schedule(Name=name)
            task_def = s["Target"]["EcsParameters"]["TaskDefinitionArn"].split("/")[-1]
            print(f"{s['Name']:<32} | {s['State']:<10} | {s['ScheduleExpression']:<22} | {task_def}")
        except ClientError as e:
            print(f"{name:<32} | NOT FOUND ({e.response['Error']['Code']})")

def set_state(enable: bool):
    client = get_client()
    new_state = "ENABLED" if enable else "DISABLED"
    for name in SCHEDULES:
        try:
            s = client.get_schedule(Name=name)
            client.update_schedule(
                Name=name,
                Description=s.get("Description", ""),
                ScheduleExpression=s["ScheduleExpression"],
                FlexibleTimeWindow=s["FlexibleTimeWindow"],
                Target=s["Target"],
                State=new_state
            )
            print(f"Schedule '{name}' set to {new_state}.")
        except ClientError as e:
            print(f"Error updating '{name}': {e}")

def main():
    parser = argparse.ArgumentParser(description="Manage SIC EventBridge Pipeline Schedules")
    parser.add_argument("action", choices=["status", "enable", "disable"], help="Action to perform")
    args = parser.parse_args()

    if args.action == "status":
        status()
    elif args.action == "enable":
        set_state(True)
    elif args.action == "disable":
        set_state(False)

if __name__ == "__main__":
    main()
