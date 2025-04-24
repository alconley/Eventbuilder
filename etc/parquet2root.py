#!/usr/bin/env python3
import os
import sys
import glob

import pyarrow.parquet as pq
import uproot
from tqdm import tqdm
import numpy as np

def parquet_to_root_stream(parquet_paths: list[str],
                           root_path: str,
                           tree_name: str,
                           batch_size: int = 100_000) -> None:
    """
    Stream one or more Parquet files into a single ROOT TTree without
    loading all data into memory. Shows progress with tqdm, including
    total elapsed time.
    """
    # Precompute batches per file
    file_batches = []
    for path in parquet_paths:
        pf_meta = pq.ParquetFile(path)
        n_rows = pf_meta.metadata.num_rows
        n_batches = (n_rows + batch_size - 1) // batch_size
        file_batches.append((path, n_batches))
    total_batches = sum(nb for _, nb in file_batches)

    # Custom bar format to show elapsed, remaining, rate, and total elapsed
    bar_format = (
        "{desc}: {percentage:3.0f}%|{bar}| "
        "{n}/{total} [{elapsed}<{remaining}, {rate_fmt}] "
        "[total: {elapsed}]"
    )

    with uproot.recreate(root_path) as root_file:
        first = True
        with tqdm(total=total_batches,
                  desc=f"Parquet → ROOT ({tree_name})",
                  bar_format=bar_format) as pbar:
            for path, _ in file_batches:
                pf = pq.ParquetFile(path)
                for batch in pf.iter_batches(batch_size=batch_size):
                    # Convert RecordBatch to numpy arrays
                    data = {
                        col: batch.column(col).to_numpy(zero_copy_only=False)
                        for col in batch.schema.names
                    }

                    if first:
                        root_file[tree_name] = data
                        first = False
                    else:
                        root_file[tree_name].extend(data)

                    pbar.update(1)

    # Print final file size
    size_bytes = os.path.getsize(root_path)
    size_mb = size_bytes / (1024**2)
    print(f"Wrote streaming TTree '{tree_name}' to {root_path} ({size_mb:.2f} MB)")

if __name__ == "__main__":
    # allow 3 or 4 args: script, glob, output.root, tree_name, [batch_size]
    if len(sys.argv) not in (4, 5):
        print("Usage: parquet_to_root.py <input_glob> <output.root> <tree_name> [batch_size]")
        sys.exit(1)

    parquet_glob = sys.argv[1]
    parquet_files = sorted(glob.glob(parquet_glob))
    if not parquet_files:
        print(f"No files match pattern: {parquet_glob}")
        sys.exit(1)
        
    print("Converting the following Parquet files:")
    for path in parquet_files:
        print(f"  {path}")

    root_file  = sys.argv[2]
    tree_name  = sys.argv[3]
    batch_size = int(sys.argv[4]) if len(sys.argv) == 5 else 100_000

    parquet_to_root_stream(
        parquet_paths=parquet_files,
        root_path=root_file,
        tree_name=tree_name,
        batch_size=batch_size
    )
    
# Requires:
#   pip install pyarrow uproot tqdm numpy