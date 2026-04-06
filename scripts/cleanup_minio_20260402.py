#!/usr/bin/env python3
"""One-shot: delete MinIO objects for kkpolice/adlibhotel 20260402 from vlm-raw bucket."""
from minio import Minio
from minio.deleteobjects import DeleteObject

client = Minio("172.168.47.36:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)

prefixes = [
    "kkpolice-event-bucket/20260402/",
    "adlibhotel-event-bucket/20260402/",
]

for prefix in prefixes:
    objects = list(client.list_objects("vlm-raw", prefix=prefix, recursive=True))
    print(f"vlm-raw {prefix}: {len(objects)} objects")
    if objects:
        delete_list = [DeleteObject(o.object_name) for o in objects]
        errors = list(client.remove_objects("vlm-raw", delete_list))
        if errors:
            for e in errors:
                print(f"  ERROR: {e}")
        else:
            print(f"  Deleted {len(delete_list)} objects")

    labels = list(client.list_objects("vlm-labels", prefix=prefix, recursive=True))
    print(f"vlm-labels {prefix}: {len(labels)} objects")
    if labels:
        delete_list = [DeleteObject(o.object_name) for o in labels]
        errors = list(client.remove_objects("vlm-labels", delete_list))
        if errors:
            for e in errors:
                print(f"  ERROR: {e}")
        else:
            print(f"  Deleted {len(delete_list)} label objects")

# Verify
for prefix in prefixes:
    remaining = list(client.list_objects("vlm-raw", prefix=prefix, recursive=True))
    print(f"VERIFY vlm-raw {prefix}: {len(remaining)} remaining")

print("MinIO cleanup complete")
