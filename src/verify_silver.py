import polars as pl
from src.config import settings


def inspect_table(name: str, path: str, partitioned: bool = False):
    print(f"--- Inspecting {name.upper()} ---")
    print(f"Path: {path}")

    try:
        if partitioned:
            # For partitioned data, we scan the whole directory
            lazy_df = pl.scan_parquet(
                path,
                storage_options=settings.polars_storage_options,
                hive_partitioning=True,
            )
            df = lazy_df.collect()
        else:
            # For single files
            df = pl.read_parquet(path, storage_options=settings.polars_storage_options)

        print(f"Total Records: {df.height}")
        print(f"Columns: {len(df.columns)}")
        print("Sample Data:")
        with pl.Config(fmt_str_lengths=30, tbl_cols=8):
            print(df.head(3))

    except Exception as e:
        print(f"Failed to read {name}: {e}")
    print("\n" + "=" * 50 + "\n")


def run():
    base_az = f"az://{settings.DATA_CONTAINER}/silver"

    # Check Fact Table
    inspect_table(
        name="Listening Events (Fact)",
        path=f"{base_az}/listening_events/**/*.parquet",
        partitioned=True,
    )

    # Check Dimension Tables
    inspect_table(name="Tracks (Dim)", path=f"{base_az}/tracks/data.parquet")
    inspect_table(name="Artists (Dim)", path=f"{base_az}/artists/data.parquet")
    inspect_table(name="Albums (Dim)", path=f"{base_az}/albums/data.parquet")


if __name__ == "__main__":
    run()
