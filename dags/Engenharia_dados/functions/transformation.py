#!/usr/bin/env python3
import sys
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def transform_parquet(source_uri, destination_uri):
    """
    Reads a Parquet file in chunks, performs transformations using pandas,
    and writes the result to a new Parquet file.
    """
    # Read the Parquet file in chunks
    parquet_file = pq.ParquetFile(source_uri)
    for batch in parquet_file.iter_batches():
        df = batch.to_pandas()

        # Perform your pandas transformations here
        # df['new_column'] = df['column1'] * 2 
        # df = df.drop(columns=['unnecessary_column'])

        # Write the transformed chunk to a new Parquet file
        if 'table' not in locals():
            table = pa.Table.from_pandas(df)
            pq.write_table(table, destination_uri)
        else:
            table = pa.Table.from_pandas(df)
            pq.write_to_dataset(table, destination_uri, append=True)

if __name__ == "__main__":
    transform_parquet(sys.argv[1], sys.argv[2])