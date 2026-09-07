from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

from .common import canonical, digest


class LocalObjects:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

    def put(self, key, data):
        target = self.directory / key
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            if target.read_bytes() != data:
                raise ValueError("Immutable archive object conflict")
            return
        with target.open("xb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())

    def get(self, key):
        return (self.directory / key).read_bytes()

    def keys(self, prefix):
        return sorted(str(p.relative_to(self.directory)) for p in (self.directory / prefix).rglob("*") if p.is_file())


class S3Objects:
    def __init__(self):
        import boto3
        self.client = boto3.client("s3", endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:19000"),
                                   aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "adpulse"),
                                   aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "adpulse-local-storage"), region_name="us-east-1")
        self.bucket = os.getenv("S3_BUCKET", "adpulse-archive")

    def put(self, key, data):
        from botocore.exceptions import ClientError
        try:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=data, IfNoneMatch="*")
        except ClientError as exc:
            if exc.response["ResponseMetadata"]["HTTPStatusCode"] not in (409, 412):
                raise
            if self.get(key) != data:
                raise ValueError("Immutable S3 archive conflict") from exc

    def get(self, key):
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def keys(self, prefix):
        pages = self.client.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix)
        return sorted(item["Key"] for page in pages for item in page.get("Contents", []))


def archive_batch(objects, records):
    """Data object first, immutable commit manifest second. Never rename on S3."""
    body = ("\n".join(canonical(r) for r in records) + "\n").encode()
    sha = hashlib.sha256(body).hexdigest()
    key = f"data/{sha}.jsonl"
    objects.put(key, body)
    manifest = {"format_version": 1, "data_key": key, "sha256": sha, "record_count": len(records),
                "records": [dict(topic=r["topic"], partition=r["partition"], offset=r["offset"],
                                 sha256=digest(r["value"])) for r in records]}
    objects.put(f"manifests/{sha}.json", canonical(manifest).encode())
    return manifest


def verified_records(objects):
    unique = {}
    for key in objects.keys("manifests/"):
        manifest = json.loads(objects.get(key))
        body = objects.get(manifest["data_key"])
        if hashlib.sha256(body).hexdigest() != manifest["sha256"]:
            raise ValueError(f"Archive checksum mismatch: {key}")
        rows = [json.loads(line) for line in body.splitlines()]
        if len(rows) != manifest["record_count"] or len(rows) != len(manifest["records"]):
            raise ValueError(f"Archive count mismatch: {key}")
        for row, ref in zip(rows, manifest["records"], strict=True):
            identity = (row["topic"], row["partition"], row["offset"])
            if identity != (ref["topic"], ref["partition"], ref["offset"]) or digest(row["value"]) != ref["sha256"]:
                raise ValueError(f"Archive offset/content mismatch: {key}")
            if identity in unique and unique[identity] != row:
                raise ValueError("Conflicting archived offset")
            unique[identity] = row
    return [unique[k] for k in sorted(unique)]


def reconcile(records, quality=(), expected_receipts=None):
    """Count actual receipt identities; Kafka offset arithmetic is not record count."""
    raw = {r["value"]["receipt_id"]: r for r in records if r["topic"].endswith(".raw")}
    manifests = [r["value"] for r in records if r["topic"].endswith(".receipts")]
    acknowledged = set(expected_receipts or ())
    for manifest in manifests:
        acknowledged.update(manifest["receipt_ids"])
        if manifest["count"] != len(manifest["records"]) or set(manifest["receipt_ids"]) != {r["receipt_id"] for r in manifest["records"]}:
            raise ValueError("Receipt manifest count/identity mismatch")
        for ref in manifest["records"]:
            if ref["receipt_id"] in raw:
                row = raw[ref["receipt_id"]]
                if (row["topic"], row["partition"], row["offset"], digest(row["value"])) != (ref["topic"], ref["partition"], ref["offset"], ref["sha256"]):
                    raise ValueError("Acknowledgement and archive disagree")
    dispositions = {}
    for row in quality:
        if row.get("disposition") == "signal":
            continue
        receipt = row["receipt_id"]
        if receipt in dispositions and dispositions[receipt] != row["disposition"]:
            raise ValueError("Receipt has conflicting lineage dispositions")
        dispositions[receipt] = row["disposition"]
    return dict(acknowledged=len(acknowledged), archived=len(acknowledged & raw.keys()),
                archive_missing=sorted(acknowledged - raw.keys()),
                classified=len(acknowledged & dispositions.keys()),
                processing_pending=sorted(acknowledged - dispositions.keys()),
                archive_complete=bool(acknowledged) and acknowledged <= raw.keys(),
                lineage_complete=bool(acknowledged) and acknowledged <= dispositions.keys())
