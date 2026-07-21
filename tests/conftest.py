from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


def write_rows(path: Path, rows: Iterable[dict[str, Any]], schema: pa.Schema) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(list(rows), schema=schema), path)
    return path
