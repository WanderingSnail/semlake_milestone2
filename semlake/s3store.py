"""Object store on Amazon S3 (or any S3-compatible endpoint) with the same counters.

``S3ObjectStore`` has the interface of ``MemoryObjectStore``: ``put``, ``get``,
``list`` and ``counters``. Every call is one real request, except ``list``,
which issues one request per page of 1,000 keys and counts each page. Keys are
stored under ``prefix`` so one bucket can hold several runs.
"""

from __future__ import annotations

from semlake.storage import Counters


class S3ObjectStore:
    def __init__(self, bucket: str, prefix: str, endpoint_url: str | None = None, client=None) -> None:
        if client is None:
            import boto3

            client = boto3.client("s3", endpoint_url=endpoint_url)
        self.client = client
        self.bucket = bucket
        self.prefix = prefix.rstrip("/") + "/"
        self.counters = Counters()

    def _key(self, key: str) -> str:
        return self.prefix + key

    def put(self, key: str, data: bytes) -> None:
        self.client.put_object(Bucket=self.bucket, Key=self._key(key), Body=data)
        self.counters.put += 1
        self.counters.put_bytes += len(data)

    def get(self, key: str) -> bytes:
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self._key(key))
        except self.client.exceptions.NoSuchKey as exc:
            raise FileNotFoundError(key) from exc
        data = response["Body"].read()
        self.counters.get += 1
        self.counters.get_bytes += len(data)
        return data

    def list(self, prefix: str = "") -> list[str]:
        keys: list[str] = []
        token = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": self._key(prefix)}
            if token:
                kwargs["ContinuationToken"] = token
            response = self.client.list_objects_v2(**kwargs)
            self.counters.list += 1
            keys.extend(item["Key"][len(self.prefix):] for item in response.get("Contents", []))
            if not response.get("IsTruncated"):
                break
            token = response["NextContinuationToken"]
        return sorted(keys)

    def upload_all(self, objects: dict[str, bytes], workers: int = 32) -> None:
        """Copy a prepared table into the bucket in parallel. Not counted: this is setup."""
        from concurrent.futures import ThreadPoolExecutor

        def one(item):
            self.client.put_object(Bucket=self.bucket, Key=self._key(item[0]), Body=item[1])

        with ThreadPoolExecutor(workers) as pool:
            list(pool.map(one, objects.items()))

    def delete_all(self) -> int:
        """Remove every object under the prefix. Not counted. Returns the number removed."""
        removed = 0
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
            batch = [{"Key": item["Key"]} for item in page.get("Contents", [])]
            if batch:
                self.client.delete_objects(Bucket=self.bucket, Delete={"Objects": batch})
                removed += len(batch)
        return removed
