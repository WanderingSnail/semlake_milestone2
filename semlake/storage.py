"""In-memory object store, Parquet helpers, and roaring deletion vectors."""

from __future__ import annotations

import base64
import io
import json
from dataclasses import dataclass
from typing import Any, Iterable

from pyroaring import BitMap

from semlake.metadata import FileMeta, Schema


def empty_dv_bytes() -> bytes:
    return BitMap().serialize()


class DeletionVector:
    """Positional tombstones stored as a roaring bitmap.

    The bitmap has the same role as an Iceberg v3 deletion vector: one vector
    per data file, positions in that file, stored next to the file. The data
    file itself stays as written. ``row_pos`` stands in for Iceberg v3 row
    lineage. The prototype does not write Puffin files.
    """

    def __init__(self, bitmap: BitMap | None = None) -> None:
        self.bitmap = BitMap() if bitmap is None else bitmap

    @classmethod
    def from_positions(cls, positions: Iterable[int]) -> DeletionVector:
        return cls(BitMap(int(pos) for pos in positions))

    @classmethod
    def from_bytes(cls, payload: bytes) -> DeletionVector:
        if not payload:
            return cls()
        return cls(BitMap.deserialize(payload))

    def union(self, other: DeletionVector) -> DeletionVector:
        """Return DV_A | DV_B."""
        return DeletionVector(self.bitmap | other.bitmap)

    def to_bytes(self) -> bytes:
        return self.bitmap.serialize()

    def __len__(self) -> int:
        return len(self.bitmap)

    def __contains__(self, position: int) -> bool:
        return int(position) in self.bitmap


def with_positions(dv_bytes: bytes, positions: Iterable[int]) -> bytes:
    """Return a serialized deletion vector with ``positions`` added."""
    dv = DeletionVector.from_bytes(dv_bytes)
    dv.bitmap.update(int(pos) for pos in positions)
    return dv.to_bytes()


@dataclass
class Counters:
    put: float = 0
    get: float = 0
    list: float = 0
    put_bytes: float = 0
    get_bytes: float = 0

    @property
    def requests(self) -> float:
        return self.put + self.get + self.list


class MemoryObjectStore:
    """S3 stand-in. Every PUT, GET, and LIST is counted for the cost model."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.counters = Counters()

    def put(self, key: str, data: bytes) -> None:
        self.objects[key] = data
        self.counters.put += 1
        self.counters.put_bytes += len(data)

    def get(self, key: str) -> bytes:
        try:
            data = self.objects[key]
        except KeyError as exc:
            raise FileNotFoundError(key) from exc
        self.counters.get += 1
        self.counters.get_bytes += len(data)
        return data

    def list(self, prefix: str = "") -> list[str]:
        keys = sorted(key for key in self.objects if key.startswith(prefix))
        self.counters.list += 1
        return keys

    def clone(self) -> MemoryObjectStore:
        """Return a copy with the same objects and zeroed counters."""
        # Byte strings are immutable, so the copy can share them.
        copied = MemoryObjectStore()
        copied.objects.update(self.objects)
        return copied


def dump_json(doc: dict) -> bytes:
    return json.dumps(doc, separators=(",", ":"), sort_keys=True).encode("utf-8")


def load_json(payload: bytes) -> dict:
    return json.loads(payload)


def file_to_doc(meta: FileMeta) -> dict[str, Any]:
    return {
        "key": meta.object_key,
        "size": meta.size_bytes,
        "token": meta.token,
        "dv": base64.b64encode(meta.dv_bytes).decode("ascii"),
    }


def file_from_doc(file_id: str, doc: dict) -> FileMeta:
    return FileMeta(
        file_id=file_id,
        object_key=doc["key"],
        size_bytes=int(doc["size"]),
        token=doc["token"],
        dv_bytes=base64.b64decode(doc["dv"]),
    )


def _arrow_type(type_name: str):
    import pyarrow as pa

    return {
        "int32": pa.int32(),
        "int64": pa.int64(),
        "double": pa.float64(),
        "float": pa.float32(),
        "string": pa.string(),
    }[type_name]


def write_parquet(rows: list[dict], schema: Schema) -> bytes:
    """Encode rows as Parquet bytes, with a hidden ``__row_id`` column first."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    names = ["__row_id", *schema.names()]
    arrays = [pa.array([int(row["__row_id"]) for row in rows], type=pa.int64())]
    for field in schema.fields:
        arrays.append(
            pa.array([row.get(field.name) for row in rows], type=_arrow_type(field.type_name))
        )
    table = pa.Table.from_arrays(arrays, names=names)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="none")
    return buffer.getvalue()


def read_parquet(blob: bytes) -> list[dict]:
    import pyarrow.parquet as pq

    return pq.read_table(io.BytesIO(blob)).to_pylist()


def materialize(store: MemoryObjectStore, snap) -> dict[int, dict]:
    """Read one snapshot back from storage.

    A live row is a Parquet record whose position is absent from that file's
    deletion vector. Columns the file was written without stay null. The same
    row id in two live files is a deletion-vector bug, so it raises.
    """
    rows: dict[int, dict] = {}
    fields = snap.schema.fields
    ordered = sorted(snap.files.items(), key=lambda item: (item[1].object_key, item[0]))
    for fid, meta in ordered:
        blob = store.get(meta.object_key)
        if blob[:4] != b"PAR1":
            continue
        hidden = DeletionVector.from_bytes(meta.dv_bytes)
        for position, record in enumerate(read_parquet(blob)):
            if position in hidden:
                continue
            rid = int(record.pop("__row_id"))
            if rid in rows:
                raise RuntimeError(f"row {rid} is live in more than one file, including {fid}")
            rows[rid] = {field.name: record.get(field.name) for field in fields}
    return rows
